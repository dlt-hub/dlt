import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.destination.capabilities import DataTypeMapper
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.normalizers import NamingConvention

from dlt.destinations.impl.sqlalchemy.configuration import (
    SqlalchemyCredentials,
    SqlalchemyClientConfiguration,
)

SqlalchemyTypeMapper: t.Type[DataTypeMapper]

try:
    from dlt.destinations.impl.sqlalchemy.type_mapper import SqlalchemyTypeMapper
except ModuleNotFoundError:
    # assign mock type mapper if no sqlalchemy
    from dlt.common.destination.capabilities import UnsupportedTypeMapper as SqlalchemyTypeMapper

if t.TYPE_CHECKING:
    # from dlt.destinations.impl.sqlalchemy.sqlalchemy_client import SqlalchemyJobClient
    from dlt.destinations.impl.sqlalchemy.sqlalchemy_job_client import SqlalchemyJobClient


class sqlalchemy(Destination[SqlalchemyClientConfiguration, "SqlalchemyJobClient"]):
    spec = SqlalchemyClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        # https://www.sqlalchemyql.org/docs/current/limits.html
        caps = DestinationCapabilitiesContext.generic_capabilities()
        caps.preferred_loader_file_format = "typed-jsonl"
        caps.supported_loader_file_formats = ["typed-jsonl", "parquet"]
        caps.preferred_staging_file_format = None
        caps.supported_staging_file_formats = []
        caps.has_case_sensitive_identifiers = True
        caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
        caps.wei_precision = (DEFAULT_NUMERIC_PRECISION, 0)
        caps.max_identifier_length = 63
        caps.max_column_identifier_length = 63
        caps.max_query_length = 32 * 1024 * 1024
        caps.is_max_query_length_in_bytes = True
        caps.max_text_data_type_length = 1024 * 1024 * 1024
        caps.is_max_text_data_type_length_in_bytes = True
        caps.supports_ddl_transactions = True
        caps.max_query_parameters = 20_0000
        caps.max_rows_per_insert = 10_000  # Set a default to avoid OOM on large datasets
        caps.type_mapper = SqlalchemyTypeMapper

        return caps

    @classmethod
    def adjust_capabilities(
        cls,
        caps: DestinationCapabilitiesContext,
        config: SqlalchemyClientConfiguration,
        naming: t.Optional[NamingConvention],
    ) -> DestinationCapabilitiesContext:
        caps = super(sqlalchemy, cls).adjust_capabilities(caps, config, naming)
        dialect = config.get_dialect()
        if dialect is None:
            return caps
        caps.max_identifier_length = dialect.max_identifier_length
        caps.max_column_identifier_length = dialect.max_identifier_length
        caps.supports_native_boolean = dialect.supports_native_boolean

        return caps

    @property
    def client_class(self) -> t.Type["SqlalchemyJobClient"]:
        from dlt.destinations.impl.sqlalchemy.sqlalchemy_job_client import SqlalchemyJobClient

        return SqlalchemyJobClient

    def __init__(
        self,
        credentials: t.Union[SqlalchemyCredentials, t.Dict[str, t.Any], str] = None,
        destination_name: t.Optional[str] = None,
        environment: t.Optional[str] = None,
        engine_args: t.Optional[t.Dict[str, t.Any]] = None,
        **kwargs: t.Any,
    ) -> None:
        """Configure the Sqlalchemy destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials: Credentials to connect to the sqlalchemy database. Can be an instance of `SqlalchemyCredentials` or
                a connection string in the format `mysql://user:password@host:port/database`
            destination_name: The name of the destination
            environment: The environment to use
            **kwargs: Additional arguments passed to the destination
        """
        super().__init__(
            credentials=credentials,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )
