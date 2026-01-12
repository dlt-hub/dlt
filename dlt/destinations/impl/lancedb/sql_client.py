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

from dlt.common.destination import PreparedTableSchema
from dlt.common.destination.dataset import DBApiCursor
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.destinations.sql_client import raise_database_error, raise_open_connection_error
from dlt.destinations.impl.duckdb.sql_client import DuckDbSqlClient
from dlt.destinations.impl.duckdb.factory import _set_duckdb_raw_capabilities

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection
    from dlt.destinations.impl.lancedb.lancedb_client import LanceDBClient


def _get_lancedb_sql_capabilities() -> DestinationCapabilitiesContext:
    caps = DestinationCapabilitiesContext()
    caps = _set_duckdb_raw_capabilities(caps)
    caps.preferred_loader_file_format = "parquet"
    return caps


def _install_and_load_lance_duckdb_extension(duckdb_con: DuckDBPyConnection) -> DuckDBPyConnection:
    """Ensure the `lance-duckdb` extension is loaded.

    DuckDB ensures installation is only done once per system.
    Extension loading must be done on every connection
    """
    duckdb_con.execute("INSTALL lance FROM community;")
    duckdb_con.execute("LOAD lance;")
    return duckdb_con


def _maybe_create_lance_duckdb_view(
    duckdb_con: DuckDBPyConnection, lance_uri, dlt_schema: dlt.Schema, table_name: str
):
    """Add the required tables as views to the duckdb in memory instance"""
    tables_with_data = dlt_schema.dlt_table_names() + dlt_schema.data_table_names(
        seen_data_only=True
    )
    if table_name in tables_with_data:
        # TODO raise for tables `dlt_schema["table_format"] != "lance"`
        from_statement = f"{lance_uri}/{table_name}.lance"
        create_view_sql = f'CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM "{from_statement}"'
        duckdb_con.execute(create_view_sql)


def _create_duckdb_views_for_lance_tables(
    duckdb_con: DuckDBPyConnection,
    lance_uri: str,
    dlt_schema: dlt.Schema,
    query: AnyStr,
) -> None:
    """Create views in DuckDB for each table found in the LanceDB destination."""
    expression = sqlglot.maybe_parse(query)
    for table in expression.find_all(exp.Table):
        if not table.this:
            continue

        _maybe_create_lance_duckdb_view(
            duckdb_con=duckdb_con,
            lance_uri=lance_uri,
            dlt_schema=dlt_schema,
            table_name=table.name,
        )


class LanceDBSQLClient(DuckDbSqlClient):
    def __init__(self, lancedb_client: LanceDBClient) -> None:
        self.lancedb_client = lancedb_client
        self.lance_uri = lancedb_client.config.lance_uri
        super().__init__(
            dataset_name=self.lancedb_client.dataset_name,
            staging_dataset_name=None,
            credentials=None,  # duckdb doesn't need special credentials
            capabilities=_get_lancedb_sql_capabilities(),
        )
        self._conn: DuckDBPyConnection | None = None
        self._table_views = set()

    @raise_open_connection_error
    def open_connection(self) -> DuckDBPyConnection:
        if self._conn:
            return self._conn

        self._conn = duckdb.connect(":memory:")
        _install_and_load_lance_duckdb_extension(self._conn)
        return self._conn

    def close_connection(self) -> None:
        if self._conn:
            self._conn = None

    @contextmanager
    @raise_database_error
    def execute_query(self, query: AnyStr, *args: Any, **kwargs: Any) -> Iterator[DBApiCursor]:
        # replace generic string placeholder by DuckDB placeholders
        if args or kwargs:
            query = query.replace("%s", "?")

        _create_duckdb_views_for_lance_tables(
            duckdb_con=self.open_connection(),
            lance_uri=self.lance_uri,
            dlt_schema=self.lancedb_client.schema,
            query=query,
        )

        with super().execute_query(query, *args, **kwargs) as cursor:
            yield cursor
