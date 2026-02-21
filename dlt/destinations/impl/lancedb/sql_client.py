"""Query LanceDB using DuckDB SQL client

LanceDB doesn't have an SQL interface. However, using a
DuckDB instance with the `lance-duckdb` extension allows
to read the `.lance` files (each maps to a single table).

This SQL client makes LanceDB compatible with the `dlt.Dataset`
inferface.
"""
from __future__ import annotations

from contextlib import contextmanager
from typing import Any, AnyStr, Iterator, TYPE_CHECKING

import sqlglot
import sqlglot.expressions as exp
import duckdb

import dlt
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from dlt.common.destination.dataset import DBApiCursor
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.destinations.sql_client import raise_database_error, raise_open_connection_error
from dlt.destinations.impl.duckdb.sql_client import DuckDbSqlClient
from dlt.destinations.impl.duckdb.factory import _set_duckdb_raw_capabilities

if TYPE_CHECKING:
    from sqlglot import expressions as sge
    from duckdb import DuckDBPyConnection

    from dlt.destinations.impl.lancedb.lancedb_client import LanceDBClient


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


def _prepare_create_view_statement(lance_table_uri: str, view_name: str) -> str:
    return f'CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM "{lance_table_uri}"'


class LanceDBSQLClient(DuckDbSqlClient):
    def __init__(self, lancedb_client: LanceDBClient) -> None:
        self.lancedb_client = lancedb_client
        super().__init__(
            dataset_name=self.lancedb_client.dataset_name,
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

        # by default, LanceDB has `dataset_name=None`. To be consistent, it uses DuckDB's
        # main schema by default
        if self.lancedb_client.dataset_name:
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

    def create_view(self, table_name: str) -> None:
        lance_table_uri = get_lance_table_uri(self.lancedb_client, table_name)
        create_view_sql = _prepare_create_view_statement(
            lance_table_uri=lance_table_uri,
            view_name=self.make_qualified_table_name(table_name),
        )
        try:
            self.open_connection().execute(create_view_sql)
        # Creating a DuckDB view will fail if the table doesn't exist in lance
        # potential edge case: a table only exists in the ephemeral DuckDB
        except duckdb.IOException as e:
            raise DatabaseUndefinedRelation(e)
