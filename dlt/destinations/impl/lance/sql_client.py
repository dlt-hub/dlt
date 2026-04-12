from __future__ import annotations

from typing import Any, Dict, TYPE_CHECKING

from dlt.destinations.exceptions import DatabaseUndefinedRelation
from dlt.destinations.sql_client import raise_database_error
from dlt.destinations.impl.duckdb.sql_client import WithTableScanners
from dlt.destinations.impl.lance.exceptions import is_lance_undefined_entity_exception

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection

    from dlt.common.destination.typing import PreparedTableSchema
    from dlt.destinations.impl.lance.lance_client import LanceClient


def _install_and_load_lance_duckdb_extension(duckdb_con: DuckDBPyConnection) -> None:
    """Ensure the `lance-duckdb` extension is loaded.

    DuckDB ensures installation is only done once per system.
    Extension loading must be done on every connection.
    """
    duckdb_con.execute("INSTALL lance;")
    duckdb_con.execute("LOAD lance;")


def _prepare_create_view_statement(lance_table_uri: str, view_name: str) -> str:
    # NOTE: direct querying fails with our Lance Directory Namespace Catalog Spec V2 table URIs, but
    # going through __lance_scan() does work
    return f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM __lance_scan('{lance_table_uri}')"


def _prepare_create_lance_secret_statement(
    secret_name: str, scope: str, storage_options: Dict[str, str]
) -> str:
    storage_options_str = "{" + ", ".join(f"'{k}': '{v}'" for k, v in storage_options.items()) + "}"
    # TODO: never_borrowed resets to True after every borrow/return cycle for external connections
    #  (WithTableScanners.memory_db). All open_connection first-time setup must be idempotent.
    return f"""
        CREATE OR REPLACE SECRET {secret_name} (
            TYPE LANCE,
            PROVIDER config,
            SCOPE '{scope}',
            STORAGE_OPTIONS {storage_options_str}
        )"""


class LanceSQLClient(WithTableScanners):
    def __init__(self, lance_client: LanceClient) -> None:
        self.lance_client = lance_client
        super().__init__(
            remote_client=lance_client,
            dataset_name=lance_client.dataset_name,
        )

    def open_connection(self) -> DuckDBPyConnection:
        with self.credentials.conn_pool._conn_lock:
            first_connection = self.credentials.conn_pool.never_borrowed
            super().open_connection()

        if first_connection:
            _install_and_load_lance_duckdb_extension(self._conn)
            self._create_lance_secret()

        return self._conn

    def can_create_view(self, table_schema: PreparedTableSchema) -> bool:
        return True

    def should_replace_view(self, view_name: str, table_schema: PreparedTableSchema) -> bool:
        # lance datasets are versioned, always refresh to get latest data
        return True

    @raise_database_error
    def create_view(self, view_name: str, table_schema: PreparedTableSchema) -> None:
        table_name = table_schema["name"]
        lance_table_uri = self.lance_client.get_table_uri(table_name)
        qualified_view = self.make_qualified_table_name(table_name)
        sql = _prepare_create_view_statement(lance_table_uri, qualified_view)
        self._conn.execute(sql)

    @raise_database_error
    def _create_lance_secret(self) -> None:
        storage_options = self.lance_client.config.storage.options
        if not storage_options:
            return
        scope = self.lance_client.config.storage.namespace_uri
        secret_name = self.create_secret_name(scope)
        stmt = _prepare_create_lance_secret_statement(secret_name, scope, storage_options)
        self._conn.execute(stmt)

    @classmethod
    def _make_database_exception(cls, ex: Exception) -> Exception:
        if is_lance_undefined_entity_exception(ex):
            return DatabaseUndefinedRelation(ex)
        return super()._make_database_exception(ex)
