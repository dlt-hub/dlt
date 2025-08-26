from typing import Any, Optional, Type, Union, Dict, TYPE_CHECKING, Sequence, Tuple

from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.data_writers.escape import escape_dremio_identifier

from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.exceptions import TerminalValueError
from dlt.common.schema.typing import TColumnSchema, TColumnType
from dlt.common.typing import TLoaderFileFormat
from dlt.destinations.type_mapping import TypeMapperImpl
from dlt.destinations.impl.dremio.configuration import (
    DremioCredentials,
    DremioClientConfiguration,
)

if TYPE_CHECKING:
    from dlt.destinations.impl.dremio.dremio import DremioClient


class DremioTypeMapper(TypeMapperImpl):
    BIGINT_PRECISION = 19
    sct_to_unbound_dbt = {
        "json": "VARCHAR",
        "text": "VARCHAR",
        "double": "DOUBLE",
        "bool": "BOOLEAN",
        "date": "DATE",
        "timestamp": "TIMESTAMP",
        "bigint": "BIGINT",
        "binary": "VARBINARY",
        "time": "TIME",
    }

    sct_to_dbt = {
        "decimal": "DECIMAL(%i,%i)",
        "wei": "DECIMAL(%i,%i)",
    }

    dbt_to_sct = {
        "VARCHAR": "text",
        "DOUBLE": "double",
        "FLOAT": "double",
        "BOOLEAN": "bool",
        "DATE": "date",
        "TIMESTAMP": "timestamp",
        "VARBINARY": "binary",
        "BINARY": "binary",
        "BINARY VARYING": "binary",
        "VARIANT": "json",
        "TIME": "time",
        "BIGINT": "bigint",
        "DECIMAL": "decimal",
    }

    def ensure_supported_type(
        self,
        column: TColumnSchema,
        table: PreparedTableSchema,
        loader_file_format: TLoaderFileFormat,
    ) -> None:
        if loader_file_format == "insert_values":
            return
        if loader_file_format == "parquet":
            # binary not supported on parquet if precision is set
            if column.get("precision") is not None and column["data_type"] == "binary":
                raise TerminalValueError(
                    "Dremio cannot load fixed width `binary` columns from parquet files. Switch to"
                    " other file format or use binary columns without precision.",
                    "binary",
                )

    def from_destination_type(
        self, db_type: str, precision: Optional[int] = None, scale: Optional[int] = None
    ) -> TColumnType:
        if db_type == "DECIMAL":
            if (precision, scale) == self.capabilities.wei_precision:
                return dict(data_type="wei")
            return dict(data_type="decimal", precision=precision, scale=scale)
        return super().from_destination_type(db_type, precision, scale)


class dremio(Destination[DremioClientConfiguration, "DremioClient"]):
    spec = DremioClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()
        caps.preferred_loader_file_format = None
        caps.supported_loader_file_formats = []
        caps.preferred_staging_file_format = "parquet"
        caps.supported_staging_file_formats = ["jsonl", "parquet", "model"]
        caps.escape_identifier = escape_dremio_identifier
        caps.type_mapper = DremioTypeMapper
        # all identifiers are case insensitive but are stored as is
        # https://docs.dremio.com/current/sonar/data-sources
        caps.has_case_sensitive_identifiers = False
        caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
        caps.wei_precision = (DEFAULT_NUMERIC_PRECISION, 0)
        caps.max_identifier_length = 255
        caps.max_column_identifier_length = 255
        caps.max_query_length = 2 * 1024 * 1024
        caps.is_max_query_length_in_bytes = True
        caps.max_text_data_type_length = 16 * 1024 * 1024
        caps.is_max_text_data_type_length_in_bytes = True
        caps.supports_transactions = False
        caps.supports_ddl_transactions = False
        caps.alter_add_multi_column = True
        caps.supports_clone_table = False
        caps.supports_multiple_statements = False
        caps.timestamp_precision = 3
        caps.supported_merge_strategies = ["delete-insert", "scd2"]
        caps.supported_replace_strategies = ["truncate-and-insert", "insert-from-staging"]
        caps.enforces_nulls_on_alter = False
        caps.sqlglot_dialect = "presto"

        return caps

    @property
    def client_class(self) -> Type["DremioClient"]:
        from dlt.destinations.impl.dremio.dremio import DremioClient

        return DremioClient

    def __init__(
        self,
        credentials: Union[DremioCredentials, Dict[str, Any], str] = None,
        staging_data_source: str = None,
        destination_name: str = None,
        environment: str = None,
        **kwargs: Any,
    ) -> None:
        """Configure the Dremio destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials (Union[DremioCredentials, Dict[str, Any], str], optional): Credentials to connect to the dremio database. Can be an instance of `DremioCredentials` or
                a connection string in the format `dremio://user:password@host:port/database`
            staging_data_source (str, optional): The name of the "Object Storage" data source in Dremio containing the s3 bucket
            destination_name (str, optional): Name of the destination, can be used in config section to differentiate between multiple of the same type
            environment (str, optional): Environment of the destination
            **kwargs (Any): Additional arguments passed to the destination config
        """
        super().__init__(
            credentials=credentials,
            staging_data_source=staging_data_source,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )


dremio.register()
