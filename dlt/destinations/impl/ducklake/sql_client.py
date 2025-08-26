from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.destinations.impl.duckdb.sql_client import DuckDbSqlClient
from dlt.destinations.impl.ducklake.configuration import DuckLakeCredentials
from dlt.destinations.sql_client import raise_open_connection_error

from duckdb import DuckDBPyConnection


class DuckLakeSqlClient(DuckDbSqlClient):
    def __init__(
        self,
        dataset_name: str,
        staging_dataset_name: str,
        credentials: DuckLakeCredentials,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(dataset_name, None, credentials, capabilities)
        self.credentials = credentials

    # TODO support connecting to a snapshot
    @raise_open_connection_error
    def open_connection(self) -> DuckDBPyConnection:
        """Ensure the `ducklake` extension is loaded. This needs to be done on every connection"""
        super().open_connection()
        self._conn.execute("LOAD ducklake;")
        self._conn.execute(self.credentials.attach_statement)
        self._conn.execute(f"USE {self.credentials.ducklake_name};")
        return self._conn
