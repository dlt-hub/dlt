import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.data_writers.escape import escape_postgres_identifier, escape_duckdb_literal
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE

from dlt.destinations.impl.duckdb.configuration import DuckDbCredentials, DuckDbClientConfiguration

if t.TYPE_CHECKING:
    from duckdb import DuckDBPyConnection
    from dlt.destinations.impl.duckdb.duck import DuckDbClient


class duckdb(Destination[DuckDbClientConfiguration, "DuckDbClient"]):
    spec = DuckDbClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()
        caps.preferred_loader_file_format = "insert_values"
        caps.supported_loader_file_formats = ["insert_values", "parquet", "jsonl"]
        caps.preferred_staging_file_format = None
        caps.supported_staging_file_formats = []
        caps.escape_identifier = escape_postgres_identifier
        # all identifiers are case insensitive but are stored as is
        caps.escape_literal = escape_duckdb_literal
        caps.has_case_sensitive_identifiers = False
        caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
        caps.wei_precision = (DEFAULT_NUMERIC_PRECISION, 0)
        caps.max_identifier_length = 65536
        caps.max_column_identifier_length = 65536
        caps.max_query_length = 32 * 1024 * 1024
        caps.is_max_query_length_in_bytes = True
        caps.max_text_data_type_length = 1024 * 1024 * 1024
        caps.is_max_text_data_type_length_in_bytes = True
        caps.supports_ddl_transactions = True
        caps.alter_add_multi_column = False
        caps.supports_truncate_command = False
        caps.supported_merge_strategies = ["delete-insert", "scd2"]

        return caps

    @property
    def client_class(self) -> t.Type["DuckDbClient"]:
        from dlt.destinations.impl.duckdb.duck import DuckDbClient

        return DuckDbClient

    def __init__(
        self,
        credentials: t.Union[
            DuckDbCredentials, t.Dict[str, t.Any], str, "DuckDBPyConnection"
        ] = None,
        create_indexes: bool = False,
        destination_name: t.Optional[str] = None,
        environment: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        """Configure the DuckDB destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials: Credentials to connect to the duckdb database. Can be an instance of `DuckDbCredentials` or
                a path to a database file. Use :pipeline: to create a duckdb
                in the working folder of the pipeline
            create_indexes: Should unique indexes be created, defaults to False
            **kwargs: Additional arguments passed to the destination config
        """
        super().__init__(
            credentials=credentials,
            create_indexes=create_indexes,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )
