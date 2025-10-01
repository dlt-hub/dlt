from typing import ClassVar, Type

from duckdb import DuckDBPyConnection

from dlt.common import logger
from dlt.common.configuration.specs.connection_string_credentials import ConnectionStringCredentials
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.storages.configuration import FileSystemCredentials
from dlt.common.storages.fsspec_filesystem import fsspec_from_config
from dlt.destinations.impl.duckdb.configuration import DuckDbCredentials
from dlt.destinations.impl.duckdb.sql_client import DuckDBDBApiCursorImpl, DuckDbSqlClient
from dlt.destinations.impl.ducklake.configuration import DuckLakeCredentials
from dlt.destinations.sql_client import raise_open_connection_error


class DuckLakeDBApiCursorImpl(DuckDBDBApiCursorImpl):
    vector_size: ClassVar[int] = 700  # vector size for ducklake


class DuckLakeSqlClient(DuckDbSqlClient):
    cursor_impl: ClassVar[Type[DuckDBDBApiCursorImpl]] = DuckLakeDBApiCursorImpl

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

    @raise_open_connection_error
    def open_connection(self) -> DuckDBPyConnection:
        """Ensure the `ducklake` extension is loaded. This needs to be done on every connection"""
        super().open_connection()
        # setup secrets and attach ducklake for each opened connections. connection pool
        # creates a separate connection for each sql_client
        try:
            if not self.credentials.storage.is_local_filesystem:
                self.create_secret(
                    self.credentials.storage.bucket_url, self.credentials.storage.credentials
                )
            # NOTE: database must be detached otherwise it is left in inconsistent state
            # TODO: perhaps move attach/detach to connection pool
            self._conn.execute(self.attach_statement)
            self._conn.execute(
                f"USE {self.capabilities.escape_identifier(self.credentials.ducklake_name)};"
            )
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

    def create_secret(
        self,
        scope: str,
        credentials: FileSystemCredentials,
        secret_name: str = None,
        persist_secrets: bool = False,
    ) -> None:
        protocol = self.credentials.storage.protocol
        if protocol in ["az", "abfss"]:
            logger.warning(
                "abfss is not supported by DuckLake. "
                "Falling back to fsspec which degrades scanning performance."
            )
            self._register_filesystem(fsspec_from_config(self.credentials.storage)[0], "abfss")
        elif not super().create_secret(
            scope,
            credentials,
            secret_name=secret_name,
            persist_secrets=persist_secrets,
        ):
            if protocol in ["gs", "gcs"]:
                logger.warning(
                    "For gs/gcs access via duckdb please use the gs/gcs s3 compatibility"
                    "layer if possible. "
                    "Falling back to fsspec which degrades scanning performance."
                )
                self._register_filesystem(fsspec_from_config(self.credentials.storage)[0], "gcs")
            else:
                raise ValueError(f"Cannot create secret or register filesystem for `{protocol=}`")

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
            # `drivername="postgresql"` is supported by dlt / sqlalchemy, but it doesn't exist in duckdb.
            if catalog.drivername == "postgresql":
                catalog.drivername = "postgres"

            db_url = catalog.to_url().render_as_string(hide_password=False)
            attach_statement = f"ATTACH IF NOT EXISTS 'ducklake:{catalog.drivername}:{db_url}'"
            attach_params = f", METADATA_SCHEMA '{ducklake_name}'"
        elif catalog.drivername == "md":
            logger.warning(
                "Motherduck requires token present in the environment and will most probably crash."
            )
            attach_statement = f"ATTACH IF NOT EXISTS 'ducklake:md:{catalog.database}'"
            attach_params = f", METADATA_SCHEMA '{ducklake_name}'"
        elif catalog.drivername in ("sqlite", "duckdb"):
            # attach sqllite with multi-process access
            attach_statement = f"ATTACH IF NOT EXISTS 'ducklake:{catalog.database}'"
            attach_params = ", META_TYPE 'sqlite', META_JOURNAL_MODE 'WAL', META_BUSY_TIMEOUT 1000"
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
