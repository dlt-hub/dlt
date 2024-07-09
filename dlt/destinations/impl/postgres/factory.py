import typing as t

from dlt.common.data_writers.configuration import CsvFormatConfiguration
from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.data_writers.escape import escape_postgres_identifier, escape_postgres_literal
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.wei import EVM_DECIMAL_PRECISION

from dlt.destinations.impl.postgres.configuration import (
    PostgresCredentials,
    PostgresClientConfiguration,
)

if t.TYPE_CHECKING:
    from dlt.destinations.impl.postgres.postgres import PostgresClient


class postgres(Destination[PostgresClientConfiguration, "PostgresClient"]):
    spec = PostgresClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        # https://www.postgresql.org/docs/current/limits.html
        caps = DestinationCapabilitiesContext()
        caps.preferred_loader_file_format = "insert_values"
        caps.supported_loader_file_formats = ["insert_values", "csv"]
        caps.preferred_staging_file_format = None
        caps.supported_staging_file_formats = []
        caps.escape_identifier = escape_postgres_identifier
        # postgres has case sensitive identifiers but by default
        # it folds them to lower case which makes them case insensitive
        # https://stackoverflow.com/questions/20878932/are-postgresql-column-names-case-sensitive
        caps.casefold_identifier = str.lower
        caps.has_case_sensitive_identifiers = True
        caps.escape_literal = escape_postgres_literal
        caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
        caps.wei_precision = (2 * EVM_DECIMAL_PRECISION, EVM_DECIMAL_PRECISION)
        caps.max_identifier_length = 63
        caps.max_column_identifier_length = 63
        caps.max_query_length = 32 * 1024 * 1024
        caps.is_max_query_length_in_bytes = True
        caps.max_text_data_type_length = 1024 * 1024 * 1024
        caps.is_max_text_data_type_length_in_bytes = True
        caps.supports_ddl_transactions = True
        caps.supported_merge_strategies = ["delete-insert", "upsert", "scd2"]

        return caps

    @property
    def client_class(self) -> t.Type["PostgresClient"]:
        from dlt.destinations.impl.postgres.postgres import PostgresClient

        return PostgresClient

    def __init__(
        self,
        credentials: t.Union[PostgresCredentials, t.Dict[str, t.Any], str] = None,
        create_indexes: bool = True,
        csv_format: t.Optional[CsvFormatConfiguration] = None,
        destination_name: t.Optional[str] = None,
        environment: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        """Configure the Postgres destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials: Credentials to connect to the postgres database. Can be an instance of `PostgresCredentials` or
                a connection string in the format `postgres://user:password@host:port/database`
            create_indexes: Should unique indexes be created
            csv_format: Formatting options for csv file format
            **kwargs: Additional arguments passed to the destination config
        """
        super().__init__(
            credentials=credentials,
            create_indexes=create_indexes,
            csv_format=csv_format,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )
