import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.data_writers.escape import escape_dremio_identifier

from dlt.destinations.impl.dremio.configuration import (
    DremioCredentials,
    DremioClientConfiguration,
)

if t.TYPE_CHECKING:
    from dlt.destinations.impl.dremio.dremio import DremioClient


class dremio(Destination[DremioClientConfiguration, "DremioClient"]):
    spec = DremioClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()
        caps.preferred_loader_file_format = None
        caps.supported_loader_file_formats = []
        caps.preferred_staging_file_format = "parquet"
        caps.supported_staging_file_formats = ["jsonl", "parquet"]
        caps.escape_identifier = escape_dremio_identifier
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
        return caps

    @property
    def client_class(self) -> t.Type["DremioClient"]:
        from dlt.destinations.impl.dremio.dremio import DremioClient

        return DremioClient

    def __init__(
        self,
        credentials: t.Union[DremioCredentials, t.Dict[str, t.Any], str] = None,
        staging_data_source: str = None,
        destination_name: t.Optional[str] = None,
        environment: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        """Configure the Dremio destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials: Credentials to connect to the dremio database. Can be an instance of `DremioCredentials` or
                a connection string in the format `dremio://user:password@host:port/database`
            staging_data_source: The name of the "Object Storage" data source in Dremio containing the s3 bucket
        """
        super().__init__(
            credentials=credentials,
            staging_data_source=staging_data_source,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )
