from dlt.common.configuration.specs.connection_string_credentials import ConnectionStringCredentials
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.destinations.impl.duckdb.configuration import DuckDbCredentials
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
        super().__init__(dataset_name, staging_dataset_name, credentials, capabilities)
        self.credentials: DuckLakeCredentials = credentials
        self._attach_statement: str = None

    def create_dataset(self) -> None:
        if self.has_dataset():
            import duckdb

            raise self._make_database_exception(
                duckdb.CatalogException(
                    f'Catalog Error: Schema with name "{self.fully_qualified_dataset_name()}"'
                    " already exists!"
                )
            )
        super().create_dataset()

    # TODO support connecting to a snapshot
    @raise_open_connection_error
    def open_connection(self) -> DuckDBPyConnection:
        """Ensure the `ducklake` extension is loaded. This needs to be done on every connection"""
        super().open_connection()
        # setup secrets and attach ducklake for each opened connections. connection pool
        # creates a separate connection for each sql_client
        try:
            if not self.credentials.storage.is_local_filesystem:
                if not super().create_secret(
                    self.credentials.storage.bucket_url, self.credentials.storage.credentials
                ):
                    protocol = self.credentials.storage.protocol
                    if protocol in ["gs", "gcs"]:
                        raise ValueError(
                            "For gs/gcs access via duckdb please use the gs/gcs s3 compatibility"
                            " layer"
                        )
                    else:
                        raise ValueError(
                            f"Cannot create secret or register filesystem for `{protocol=:}`"
                        )
            # NOTE: database must be detached otherwise it is left in inconsistent state
            # TODO: perhaps move attach/detach to connection pool
            self._conn.execute(self.attach_statement)
            self._conn.execute(f"USE {self.credentials.ducklake_name};")
            # search path can only by set after database is attached
            try:
                self._conn.execute(f"SET search_path = '{self.fully_qualified_dataset_name()}'")
            except Exception:
                pass
        except Exception:
            self.close_connection()
            raise
        return self._conn

    def close_connection(self) -> None:
        if self._conn:
            # metadata database MUST be detached properly before connection is closed, we observed
            # corrupted duckdb catalogs if not done properly
            try:
                # rollback any pending transaction
                self._conn.execute("ROLLBACK;")
            except Exception:
                pass
            # make sure catalog is attached
            current_db = self._conn.sql("SELECT current_database();").fetchone()[0]
            if current_db == self.credentials.ducklake_name:
                # `memory` is a name of initial memory database opened
                self._conn.execute(f"USE memory;DETACH {self.credentials.ducklake_name}")
        return super().close_connection()

    @staticmethod
    def build_attach_statement(
        *,
        ducklake_name: str,
        catalog: ConnectionStringCredentials,
        storage_url: str,
    ) -> str:
        attach_params = ""
        if isinstance(catalog, DuckDbCredentials):
            attach_statement = f"ATTACH IF NOT EXISTS 'ducklake:{catalog._conn_str()}'"
        elif catalog.drivername in ("postgres", "postgresql", "mysql"):
            attach_statement = f"ATTACH IF NOT EXISTS 'ducklake:postgres:{catalog.to_url()}'"
        elif catalog.drivername in ("sqlite", "duckdb"):
            # attach sqllite with multi-process access
            attach_statement = f"ATTACH IF NOT EXISTS 'ducklake:{catalog.database}'"
            attach_params = (
                ", META_TYPE 'sqlite', META_JOURNAL_MODE 'WAL', META_BUSY_TIMEOUT 1000,"
                " META_SYNCHRONOUS 'NORMAL'"
            )
        else:
            raise NotImplementedError(str(catalog))
        attach_statement += f" AS {ducklake_name}"
        attach_statement += f" (DATA_PATH '{storage_url}'{attach_params})"
        return attach_statement

    @property
    def attach_statement(self) -> str:
        # return value when set explicitly
        if self._attach_statement:
            return self._attach_statement
        else:
            return self.build_attach_statement(
                ducklake_name=self.credentials.ducklake_name,
                catalog=self.credentials.catalog,
                storage_url=self.credentials.storage_url,
            )

    @attach_statement.setter
    def attach_statement(self, value: str) -> None:
        self._attach_statement = value
