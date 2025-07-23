from typing import Any, Optional, Type, Union, Dict, TYPE_CHECKING, Sequence, Tuple

from dlt.common.data_types.typing import TDataType
from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.data_writers.escape import escape_databricks_identifier, escape_databricks_literal
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.exceptions import TerminalValueError
from dlt.common.schema.typing import TColumnSchema, TColumnType, TTableSchema
from dlt.common.typing import TLoaderFileFormat

from dlt.destinations.type_mapping import TypeMapperImpl
from dlt.destinations.impl.databricks.configuration import (
    DatabricksCredentials,
    DatabricksClientConfiguration,
)

if TYPE_CHECKING:
    from dlt.destinations.impl.databricks.databricks import DatabricksClient


class DatabricksTypeMapper(TypeMapperImpl):
    sct_to_unbound_dbt = {
        "json": "STRING",  # Json type stored as string
        "text": "STRING",
        "double": "DOUBLE",
        "bool": "BOOLEAN",
        "date": "DATE",
        "timestamp": "TIMESTAMP",  # TIMESTAMP for local timezone
        "bigint": "BIGINT",
        "binary": "BINARY",
        "decimal": "DECIMAL",  # DECIMAL(p,s) format
        "time": "STRING",
    }

    dbt_to_sct = {
        "STRING": "text",
        "DOUBLE": "double",
        "BOOLEAN": "bool",
        "DATE": "date",
        "TIMESTAMP": "timestamp",
        "BIGINT": "bigint",
        "INT": "bigint",
        "SMALLINT": "bigint",
        "TINYINT": "bigint",
        "BINARY": "binary",
        "DECIMAL": "decimal",
    }

    sct_to_dbt = {
        "decimal": "DECIMAL(%i,%i)",
        "wei": "DECIMAL(%i,%i)",
    }

    def ensure_supported_type(
        self,
        column: TColumnSchema,
        table: PreparedTableSchema,
        loader_file_format: TLoaderFileFormat,
    ) -> None:
        if loader_file_format == "jsonl" and column["data_type"] in {
            "decimal",
            "wei",
            "binary",
            "json",
            "date",
        }:
            raise TerminalValueError("", column["data_type"])

    def to_db_integer_type(self, column: TColumnSchema, table: PreparedTableSchema = None) -> str:
        precision = column.get("precision")
        if precision is None:
            return "BIGINT"
        if precision <= 8:
            return "TINYINT"
        if precision <= 16:
            return "SMALLINT"
        if precision <= 32:
            return "INT"
        if precision <= 64:
            return "BIGINT"
        raise TerminalValueError(
            f"bigint with `{precision=:}` can't be mapped to Databricks integer type"
        )

    def from_destination_type(
        self, db_type: str, precision: Optional[int] = None, scale: Optional[int] = None
    ) -> TColumnType:
        # precision and scale arguments here are meaningless as they're not included separately in information schema
        # We use full_data_type from databricks which is either in form "typename" or "typename(precision, scale)"
        type_parts = db_type.split("(")
        if len(type_parts) > 1:
            db_type = type_parts[0]
            scale_str = type_parts[1].strip(")")
            precision, scale = [int(val) for val in scale_str.split(",")]
        else:
            scale = precision = None
        db_type = db_type.upper()
        if db_type == "DECIMAL":
            if (precision, scale) == self.wei_precision():
                return dict(data_type="wei", precision=precision, scale=scale)
        return super().from_destination_type(db_type, precision, scale)


class databricks(Destination[DatabricksClientConfiguration, "DatabricksClient"]):
    spec = DatabricksClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()
        caps.preferred_loader_file_format = "parquet"
        caps.supported_loader_file_formats = ["jsonl", "parquet", "model"]
        caps.preferred_staging_file_format = "parquet"
        caps.supported_staging_file_formats = ["jsonl", "parquet"]
        caps.supported_table_formats = ["delta"]
        caps.type_mapper = DatabricksTypeMapper
        caps.escape_identifier = escape_databricks_identifier
        # databricks identifiers are case insensitive and stored in lower case
        # https://docs.databricks.com/en/sql/language-manual/sql-ref-identifiers.html
        caps.escape_literal = escape_databricks_literal
        caps.casefold_identifier = str.lower
        caps.has_case_sensitive_identifiers = False
        caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
        caps.wei_precision = (DEFAULT_NUMERIC_PRECISION, 0)
        caps.max_identifier_length = 255
        caps.max_column_identifier_length = 255
        caps.max_query_length = 2 * 1024 * 1024
        caps.is_max_query_length_in_bytes = True
        caps.max_text_data_type_length = 16 * 1024 * 1024
        caps.is_max_text_data_type_length_in_bytes = True
        caps.supports_ddl_transactions = False
        caps.supports_truncate_command = True
        # caps.supports_transactions = False
        caps.alter_add_multi_column = True
        caps.supports_multiple_statements = False
        caps.supports_clone_table = True
        caps.supported_merge_strategies = ["delete-insert", "upsert", "scd2"]
        caps.supported_replace_strategies = [
            "truncate-and-insert",
            "insert-from-staging",
            "staging-optimized",
        ]
        caps.sqlglot_dialect = "databricks"

        return caps

    @property
    def client_class(self) -> Type["DatabricksClient"]:
        from dlt.destinations.impl.databricks.databricks import DatabricksClient

        return DatabricksClient

    def __init__(
        self,
        credentials: Union[DatabricksCredentials, Dict[str, Any], str] = None,
        is_staging_external_location: bool = False,
        staging_credentials_name: str = None,
        destination_name: str = None,
        environment: str = None,
        staging_volume_name: str = None,
        create_indexes: bool = False,
        **kwargs: Any,
    ) -> None:
        """Configure the Databricks destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials (Union[DatabricksCredentials, Dict[str, Any], str], optional): Credentials to connect to the databricks database. Can be an instance of `DatabricksCredentials` or
                a connection string in the format `databricks://user:password@host:port/database`
            is_staging_external_location (bool, optional): If true, the temporary credentials are not propagated to the COPY command
            staging_credentials_name (str, optional): If set, credentials with given name will be used in copy command
            destination_name (str, optional): Name of the destination, can be used in config section to differentiate between multiple of the same type
            environment (str, optional): Environment of the destination
            staging_volume_name (str, optional): Name of the staging volume to use
            create_indexes (bool, optional): Whether PRIMARY KEY or FOREIGN KEY constrains should be created
            **kwargs (Any): Additional arguments passed to the destination config
        """
        super().__init__(
            credentials=credentials,
            is_staging_external_location=is_staging_external_location,
            staging_credentials_name=staging_credentials_name,
            destination_name=destination_name,
            environment=environment,
            staging_volume_name=staging_volume_name,
            create_indexes=create_indexes,
            **kwargs,
        )


databricks.register()
