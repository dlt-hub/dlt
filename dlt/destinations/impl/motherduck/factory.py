from typing import Any, Optional, Type, Union, Dict, TYPE_CHECKING

from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.data_writers.escape import escape_postgres_identifier, escape_duckdb_literal
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE

from dlt.destinations.impl.duckdb.factory import DuckDbTypeMapper
from dlt.destinations.impl.motherduck.configuration import (
    MotherDuckCredentials,
    MotherDuckClientConfiguration,
)

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection
    from dlt.destinations.impl.motherduck.motherduck import MotherDuckClient
else:
    DuckDBPyConnection = Any
    MotherDuckClient = Any


class motherduck(Destination[MotherDuckClientConfiguration, "MotherDuckClient"]):
    spec = MotherDuckClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()
        caps.preferred_loader_file_format = "parquet"
        caps.supported_loader_file_formats = ["parquet", "insert_values", "jsonl", "model"]
        caps.type_mapper = DuckDbTypeMapper
        caps.escape_identifier = escape_postgres_identifier
        # all identifiers are case insensitive but are stored as is
        caps.escape_literal = escape_duckdb_literal
        caps.has_case_sensitive_identifiers = False
        caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
        caps.wei_precision = (DEFAULT_NUMERIC_PRECISION, 0)
        caps.max_identifier_length = 65536
        caps.max_column_identifier_length = 65536
        caps.max_query_length = 512 * 1024
        caps.is_max_query_length_in_bytes = True
        caps.max_text_data_type_length = 1024 * 1024 * 1024
        caps.is_max_text_data_type_length_in_bytes = True
        caps.supports_ddl_transactions = True
        caps.alter_add_multi_column = False
        caps.supports_truncate_command = False
        caps.supported_merge_strategies = ["delete-insert", "scd2"]
        caps.max_parallel_load_jobs = 8
        caps.supported_replace_strategies = ["truncate-and-insert", "insert-from-staging"]
        caps.sqlglot_dialect = "duckdb"

        return caps

    @property
    def client_class(self) -> Type["MotherDuckClient"]:
        from dlt.destinations.impl.motherduck.motherduck import MotherDuckClient

        return MotherDuckClient

    def __init__(
        self,
        credentials: Union[MotherDuckCredentials, str, Dict[str, Any], DuckDBPyConnection] = None,
        create_indexes: bool = False,
        destination_name: str = None,
        environment: str = None,
        **kwargs: Any,
    ) -> None:
        """Configure the MotherDuck destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials (Union[MotherDuckCredentials, str, Dict[str, Any], DuckDBPyConnection], optional): Credentials to connect to the MotherDuck database. Can be an instance of `MotherDuckCredentials` or
                a connection string in the format `md:///<database_name>?token=<service token>`.
                Instance of `DuckDbCredentials` allows to pass access token, extensions, configs and pragmas to be set up for connection.
            create_indexes (bool, optional): Should unique indexes be created
            destination_name (str, optional): Name of the destination, can be used in config section to differentiate between multiple of the same type
            environment (str, optional): Environment of the destination
            **kwargs (Any): Additional arguments passed to the destination config
        """
        super().__init__(
            credentials=credentials,
            create_indexes=create_indexes,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )


motherduck.register()
