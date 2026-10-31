from __future__ import annotations

from typing import Any, Dict, List, TYPE_CHECKING, Optional, Tuple

from dlt.common.schema.schema import Schema
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from dlt.destinations.sql_client import raise_database_error
from dlt.destinations.impl.duckdb.configuration import ConnStatement
from dlt.destinations.impl.duckdb.sql_client import WithTableScanners
from dlt.destinations.impl.lance.exceptions import is_lance_undefined_entity_exception

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection

    from dlt.common.destination.typing import PreparedTableSchema
    from dlt.destinations.impl.lance.lance_client import LanceClient


def _prepare_create_lance_secret_statement(
    secret_name: str, scope: str, storage_options: Dict[str, str]
) -> str:
    storage_options_str = "{" + ", ".join(f"'{k}': '{v}'" for k, v in storage_options.items()) + "}"
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
        # schema-less (no dataset_name): host the read views in the ephemeral duckdb `main` schema
        super().__init__(
            remote_client=lance_client,
            dataset_name=lance_client.dataset_name or "main",
        )
        self.credentials.conn_pool.add_statements(
            [ConnStatement(sql) for sql in self._attach_extension_statements()]
        )

    def open_connection(self) -> DuckDBPyConnection:
        super().open_connection()
        # not a pool statement: object-store credentials can come from a provider chain that
        # refreshes them, and a recorded statement replays an expired credential
        self._create_lance_secret()
        return self._conn

    def can_create_view(self, table_schema: PreparedTableSchema) -> bool:
        return True

    def should_replace_view(self, view_name: str, table_schema: PreparedTableSchema) -> bool:
        return self.lance_client.config.always_refresh_views

    def create_views_for_tables(self, tables: Dict[str, str]) -> None:
        # lance extension caches datasets so new data is not visible
        # automatically, we duplicate connection to clear the cache
        if self.lance_client.config.always_refresh_views:
            self._conn = self.memory_db.duplicate()
        super().create_views_for_tables(tables)

    def create_view_select(
        self, table_schema: PreparedTableSchema, schema: Schema = None
    ) -> Optional[Tuple[str, str]]:
        table_name = table_schema["name"]
        lance_table_uri = self.lance_client.get_table_uri(table_name)
        # qualified_view = self.make_qualified_table_name(table_name)
        # NOTE: direct querying fails with our Lance Directory Namespace Catalog Spec V2 table URIs, but
        # going through __lance_scan() does work
        return lance_table_uri, f"SELECT * FROM __lance_scan('{lance_table_uri}')"

    @raise_database_error
    def _create_lance_secret(self) -> None:
        storage_options = self.lance_client.config.storage_options
        if not storage_options:
            return
        scope = self.lance_client.config.storage.namespace_uri
        secret_name = self.create_secret_name(scope)
        stmt = _prepare_create_lance_secret_statement(secret_name, scope, storage_options)
        self._conn.execute(stmt)

    def _attach_extension_statements(self) -> List[str]:
        return ["INSTALL lance;", "LOAD lance;"]

    def _attach_secret_statements(self) -> List[str]:
        storage_options = self.lance_client.config.storage_options
        if not storage_options:
            return []
        scope = self.lance_client.config.storage.namespace_uri
        return [
            _prepare_create_lance_secret_statement(
                self.create_secret_name(scope), scope, storage_options
            )
        ]

    @classmethod
    def _make_database_exception(cls, ex: Exception) -> Exception:
        if is_lance_undefined_entity_exception(ex):
            return DatabaseUndefinedRelation(ex)
        return super()._make_database_exception(ex)
