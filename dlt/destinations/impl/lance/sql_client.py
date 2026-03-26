"""Query LanceDB using DuckDB SQL client

LanceDB doesn't have an SQL interface. However, using a
DuckDB instance with the `lance-duckdb` extension allows
to read the `.lance` files (each maps to a single table).

This SQL client makes LanceDB compatible with the `dlt.Dataset`
inferface.
"""
from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator, TYPE_CHECKING

import sqlglot
import sqlglot.expressions as exp
import duckdb

from dlt.destinations.exceptions import DatabaseUndefinedRelation
from dlt.common.destination.dataset import DBApiCursor
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.destinations.sql_client import raise_database_error, raise_open_connection_error
from dlt.destinations.impl.duckdb.sql_client import DuckDbSqlClient
from dlt.destinations.impl.duckdb.factory import _set_duckdb_raw_capabilities
from dlt.destinations.impl.lance.exceptions import is_lance_undefined_entity_exception

if TYPE_CHECKING:
    from sqlglot import expressions as sge
    from duckdb import DuckDBPyConnection

    from dlt.destinations.impl.lance.lance_client import LanceClient


def _get_lancedb_sql_capabilities() -> DestinationCapabilitiesContext:
    caps = DestinationCapabilitiesContext()
    caps = _set_duckdb_raw_capabilities(caps)
    caps.preferred_loader_file_format = "parquet"
    return caps


def _install_and_load_lance_duckdb_extension(duckdb_con: DuckDBPyConnection) -> None:
    """Ensure the `lance-duckdb` extension is loaded.

    DuckDB ensures installation is only done once per system.
    Extension loading must be done on every connection
    """
    duckdb_con.execute("INSTALL lance FROM community;")
    duckdb_con.execute("LOAD lance;")


def _create_and_use_duckdb_dataset(
    duckdb_con: DuckDBPyConnection, dataset_qualified_name: str
) -> None:
    """Create a schema in the ephemeral DuckDB client that matches the `dlt` dataset name."""
    create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {dataset_qualified_name}"
    duckdb_con.execute(f"{create_schema_sql}; USE {dataset_qualified_name}")


def _prepare_create_view_statement(lance_table_uri: str, view_name: str) -> str:
    # NOTE: direct querying fails with our Lance Directory Namespace Catalog Spec V2 table URIs, but
    # going through __lance_scan() does work
    return f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM __lance_scan('{lance_table_uri}')"


class LanceSQLClient(DuckDbSqlClient):
    def __init__(self, lance_client: LanceClient) -> None:
        self.lance_client = lance_client
        super().__init__(
            dataset_name=self.lance_client.dataset_name,
            staging_dataset_name=None,
            credentials=None,  # duckdb doesn't need special credentials
            capabilities=_get_lancedb_sql_capabilities(),
        )
        self._conn: DuckDBPyConnection | None = None

    @raise_open_connection_error
    def open_connection(self) -> DuckDBPyConnection:
        if self._conn:
            return self._conn

        self._conn = duckdb.connect(":memory:")
        _install_and_load_lance_duckdb_extension(self._conn)
        _create_and_use_duckdb_dataset(self._conn, self.fully_qualified_dataset_name())

        return self._conn

    def close_connection(self) -> None:
        if self._conn:
            self._conn = None

    @contextmanager
    @raise_database_error
    def execute_query(
        self, query: str, *args: Any, **kwargs: Any  # type: ignore[override]
    ) -> Iterator[DBApiCursor]:
        # replace generic string placeholder by DuckDB placeholders
        if args or kwargs:
            query = query.replace("%s", "?")

        expression: sge.Expression = sqlglot.maybe_parse(query)
        for table in expression.find_all(exp.Table):
            if not table.this:
                continue

            self.create_view(table.name)

        with super().execute_query(query, *args, **kwargs) as cursor:
            yield cursor

    @raise_database_error
    def create_view(self, table_name: str) -> None:
        create_view_sql = _prepare_create_view_statement(
            lance_table_uri=self.lance_client.get_table_uri(table_name),
            view_name=self.make_qualified_table_name(table_name),
        )
        self.open_connection().execute(create_view_sql)

    @classmethod
    def _make_database_exception(cls, ex: Exception) -> Exception:
        if is_lance_undefined_entity_exception(ex):
            return DatabaseUndefinedRelation(ex)
        return super()._make_database_exception(ex)
