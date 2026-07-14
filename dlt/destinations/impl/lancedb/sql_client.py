"""Query LanceDB using DuckDB SQL client

LanceDB doesn't have an SQL interface. However, using a
DuckDB instance with the `lance-duckdb` extension allows
to read the `.lance` files (each maps to a single table).

This SQL client makes LanceDB compatible with the `dlt.Dataset`
inferface.
"""
from __future__ import annotations

from packaging import version as pkg_version
from typing import Dict, Optional, Tuple, TYPE_CHECKING

import duckdb

from dlt.common.schema import Schema
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from dlt.destinations.impl.duckdb.sql_client import WithTableScanners

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection

    from dlt.common.destination.typing import PreparedTableSchema
    from dlt.destinations.impl.lancedb.lancedb_client import LanceDBClient


def _install_and_load_lance_duckdb_extension(duckdb_con: DuckDBPyConnection) -> None:
    """Ensure the `lance-duckdb` extension is loaded.

    DuckDB ensures installation is only done once per system.
    Extension loading must be done on every connection
    """
    duckdb_version = pkg_version.parse(duckdb.__version__)
    if duckdb_version >= pkg_version.Version("1.5.0"):
        install_extension_cmd = "INSTALL lance;"
    else:
        install_extension_cmd = "INSTALL lance FROM community;"

    duckdb_con.execute(install_extension_cmd)
    duckdb_con.execute("LOAD lance;")


def get_lance_table_uri(lancedb_client: LanceDBClient, table_name: str) -> str:
    """Create a URI for a Lance table

    This should be equivalent to
    ```python
    lancedb_client.credentials.get_conn().open_table("foo").to_lance().uri
    ```
    """
    dataset_lance_uri = lancedb_client.config.lance_uri
    qualified_table_name = lancedb_client.make_qualified_table_name(table_name)
    return f"{dataset_lance_uri}/{qualified_table_name}.lance"


class LanceDBSQLClient(WithTableScanners):
    def __init__(self, lancedb_client: LanceDBClient) -> None:
        self.lancedb_client = lancedb_client
        # schema-less (no dataset_name): host the read views in the ephemeral duckdb `main` schema
        super().__init__(
            remote_client=lancedb_client, dataset_name=lancedb_client.dataset_name or "main"
        )

    def open_connection(self) -> DuckDBPyConnection:
        with self.credentials.conn_pool._conn_lock:
            first_connection = self.credentials.conn_pool.never_borrowed
            super().open_connection()

        if first_connection:
            _install_and_load_lance_duckdb_extension(self._conn)

        return self._conn

    def can_create_view(self, table_schema: PreparedTableSchema) -> bool:
        return True

    def should_replace_view(self, view_name: str, table_schema: PreparedTableSchema) -> bool:
        # views must be refreshed when schema evolves
        return self.lancedb_client.config.always_refresh_views

    def create_views_for_tables(self, tables: Dict[str, str]) -> None:
        # lance extension caches datasets so new data is not visible
        # automatically, we duplicate connection to clear the cache
        if self.lancedb_client.config.always_refresh_views:
            self._conn = self.memory_db.duplicate()
        super().create_views_for_tables(tables)

    def create_view_select(
        self, table_schema: PreparedTableSchema, schema: Schema = None
    ) -> Optional[Tuple[str, str]]:
        table_name = table_schema["name"]
        lance_table_uri = get_lance_table_uri(self.lancedb_client, table_name)
        # the `lance` duckdb extension reads a `.lance` directory directly
        return lance_table_uri, f'SELECT * FROM "{lance_table_uri}"'

    @classmethod
    def _make_database_exception(cls, ex: Exception) -> Exception:
        # a missing `.lance` directory means the table was not created yet in lancedb
        if isinstance(ex, duckdb.IOException):
            return DatabaseUndefinedRelation(ex)
        return super()._make_database_exception(ex)
