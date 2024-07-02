import typing as t

from dlt.common.data_writers.configuration import CsvFormatConfiguration
from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.data_writers.escape import escape_snowflake_identifier
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE

from dlt.destinations.impl.snowflake.configuration import (
    SnowflakeCredentials,
    SnowflakeClientConfiguration,
)

if t.TYPE_CHECKING:
    from dlt.destinations.impl.snowflake.snowflake import SnowflakeClient


class snowflake(Destination[SnowflakeClientConfiguration, "SnowflakeClient"]):
    spec = SnowflakeClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()
        caps.preferred_loader_file_format = "jsonl"
        caps.supported_loader_file_formats = ["jsonl", "parquet", "csv"]
        caps.preferred_staging_file_format = "jsonl"
        caps.supported_staging_file_formats = ["jsonl", "parquet", "csv"]
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
        return caps

    @property
    def client_class(self) -> t.Type["SnowflakeClient"]:
        from dlt.destinations.impl.snowflake.snowflake import SnowflakeClient

        return SnowflakeClient

    def __init__(
        self,
        credentials: t.Union[SnowflakeCredentials, t.Dict[str, t.Any], str] = None,
        stage_name: t.Optional[str] = None,
        keep_staged_files: bool = True,
        csv_format: t.Optional[CsvFormatConfiguration] = None,
        destination_name: t.Optional[str] = None,
        environment: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        """Configure the Snowflake destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials: Credentials to connect to the snowflake database. Can be an instance of `SnowflakeCredentials` or
                a connection string in the format `snowflake://user:password@host:port/database`
            stage_name: Name of an existing stage to use for loading data. Default uses implicit stage per table
            keep_staged_files: Whether to delete or keep staged files after loading
        """
        super().__init__(
            credentials=credentials,
            stage_name=stage_name,
            keep_staged_files=keep_staged_files,
            csv_format=csv_format,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )
