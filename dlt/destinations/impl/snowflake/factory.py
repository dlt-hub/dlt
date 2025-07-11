from typing import Any, Dict, Type, Union, TYPE_CHECKING, Optional

from dlt.common.destination.configuration import CsvFormatConfiguration
from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.data_writers.escape import escape_snowflake_identifier
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.exceptions import TerminalValueError
from dlt.common.schema.typing import TColumnSchema, TColumnType

from dlt.destinations.type_mapping import TypeMapperImpl
from dlt.destinations.impl.snowflake.configuration import (
    SnowflakeCredentials,
    SnowflakeClientConfiguration,
)

if TYPE_CHECKING:
    from dlt.destinations.impl.snowflake.snowflake import SnowflakeClient


class SnowflakeTypeMapper(TypeMapperImpl):
    BIGINT_PRECISION = 19
    sct_to_unbound_dbt = {
        "json": "VARIANT",
        "text": "VARCHAR",
        "double": "FLOAT",
        "bool": "BOOLEAN",
        "date": "DATE",
        "timestamp": "TIMESTAMP_TZ",
        "bigint": f"NUMBER({BIGINT_PRECISION},0)",  # Snowflake has no integer types
        "binary": "BINARY",
        "time": "TIME",
    }

    sct_to_dbt = {
        "text": "VARCHAR(%i)",
        "timestamp": "TIMESTAMP_TZ(%i)",
        "decimal": "NUMBER(%i,%i)",
        "time": "TIME(%i)",
        "wei": "NUMBER(%i,%i)",
    }

    dbt_to_sct = {
        "VARCHAR": "text",
        "FLOAT": "double",
        "BOOLEAN": "bool",
        "DATE": "date",
        "TIMESTAMP_TZ": "timestamp",
        "BINARY": "binary",
        "VARIANT": "json",
        "TIME": "time",
    }

    def from_destination_type(
        self, db_type: str, precision: Optional[int] = None, scale: Optional[int] = None
    ) -> TColumnType:
        if db_type == "NUMBER":
            if precision == self.BIGINT_PRECISION and scale == 0:
                return dict(data_type="bigint")
            elif (precision, scale) == self.capabilities.wei_precision:
                return dict(data_type="wei")
            return dict(data_type="decimal", precision=precision, scale=scale)
        if db_type == "TIMESTAMP_NTZ":
            return dict(data_type="timestamp", precision=precision, scale=scale, timezone=False)
        return super().from_destination_type(db_type, precision, scale)

    def to_db_datetime_type(
        self,
        column: TColumnSchema,
        table: PreparedTableSchema = None,
    ) -> str:
        timezone = column.get("timezone", True)
        precision = column.get("precision")

        if timezone and precision is None:
            return None

        timestamp = "TIMESTAMP_TZ" if timezone else "TIMESTAMP_NTZ"

        # append precision if specified and valid
        if precision is not None:
            if 0 <= precision <= 9:
                timestamp += f"({precision})"
            else:
                column_name = column["name"]
                table_name = table["name"]
                raise TerminalValueError(
                    f"Snowflake does not support `{precision=:}` for timestamp column"
                    f" `{column_name}` in table `{table_name}`"
                )

        return timestamp


class snowflake(Destination[SnowflakeClientConfiguration, "SnowflakeClient"]):
    spec = SnowflakeClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()
        caps.preferred_loader_file_format = "jsonl"
        caps.supported_loader_file_formats = ["jsonl", "parquet", "csv", "model"]
        caps.preferred_staging_file_format = "jsonl"
        caps.supported_staging_file_formats = ["jsonl", "parquet", "csv"]
        caps.type_mapper = SnowflakeTypeMapper
        # snowflake is case sensitive but all unquoted identifiers are upper cased
        # so upper case identifiers are considered case insensitive
        caps.escape_identifier = escape_snowflake_identifier
        # dlt is configured to create case insensitive identifiers
        # note that case sensitive naming conventions will change this setting to "str" (case sensitive)
        caps.casefold_identifier = str.upper
        caps.has_case_sensitive_identifiers = True
        caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
        caps.wei_precision = (DEFAULT_NUMERIC_PRECISION, 0)
        caps.max_identifier_length = 255
        caps.max_column_identifier_length = 255
        caps.max_query_length = 2 * 1024 * 1024
        caps.is_max_query_length_in_bytes = True
        caps.max_text_data_type_length = 16 * 1024 * 1024
        caps.is_max_text_data_type_length_in_bytes = True
        caps.supports_ddl_transactions = True
        caps.alter_add_multi_column = True
        caps.supports_clone_table = True
        caps.supported_merge_strategies = ["delete-insert", "upsert", "scd2"]
        caps.supported_replace_strategies = [
            "truncate-and-insert",
            "insert-from-staging",
            "staging-optimized",
        ]
        caps.sqlglot_dialect = "snowflake"

        return caps

    @property
    def client_class(self) -> Type["SnowflakeClient"]:
        from dlt.destinations.impl.snowflake.snowflake import SnowflakeClient

        return SnowflakeClient

    def __init__(
        self,
        credentials: Union[SnowflakeCredentials, Dict[str, Any], str] = None,
        stage_name: Optional[str] = None,
        keep_staged_files: bool = True,
        csv_format: Optional[CsvFormatConfiguration] = None,
        query_tag: Optional[str] = None,
        create_indexes: bool = False,
        destination_name: str = None,
        environment: str = None,
        **kwargs: Any,
    ) -> None:
        """Configure the Snowflake destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials (Union[SnowflakeCredentials, Dict[str, Any], str], optional): Credentials to connect to the snowflake database. Can be an instance of `SnowflakeCredentials` or
                a connection string in the format `snowflake://user:password@host:port/database`
            stage_name (Optional[str], optional): Name of an existing stage to use for loading data. Default uses implicit stage per table
            keep_staged_files (bool, optional): Whether to delete or keep staged files after loading
            csv_format (Optional[CsvFormatConfiguration]): Optional csv format configuration
            query_tag (Optional[str]): A tag with placeholders to tag sessions executing jobs
            create_indexes (bool, optional): Whether UNIQUE or PRIMARY KEY constrains should be created
            destination_name (str, optional): Name of the destination. Defaults to None.
            environment (str, optional): Environment name. Defaults to None.
            **kwargs (Any, optional): Additional arguments forwarded to the destination config
        """
        super().__init__(
            credentials=credentials,
            stage_name=stage_name,
            keep_staged_files=keep_staged_files,
            csv_format=csv_format,
            query_tag=query_tag,
            create_indexes=create_indexes,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )


snowflake.register()
