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


class LanceDBSQLClient(DuckDbSqlClient):
    def __init__(
        self,
        lancedb_client: LanceDBClient,
    ) -> None:
        self.lancedb_client = lancedb_client
        self.lance_uri = lancedb_client.config.lance_uri
        super().__init__(
            dataset_name=self.lancedb_client.dataset_name,
            staging_dataset_name=None,
            credentials=None,
            capabilities=_get_lancedb_sql_capabilities(),
        )

    @raise_open_connection_error
    def open_connection(self) -> DuckDBPyConnection:
        """Ensure the `lance-duckdb` extension is loaded. This needs to be done on every connection"""
        self._conn = duckdb.connect(":memory:")
        # only executes the first time on the system
        self._conn.execute("INSTALL lance FROM community;")
        self._conn.execute("LOAD lance;")
        return self._conn

    def close_connection(self) -> None:
        if self._conn:
            self._conn = None

    @contextmanager
    @raise_database_error
    def execute_query(self, query: AnyStr, *args: Any, **kwargs: Any) -> Iterator[DBApiCursor]:
        if args or kwargs:
            query = query.replace("%s", "?")

        expression = sqlglot.parse_one(query, read="duckdb")
        load_tables: dict[str, str] = {}
        for table in expression.find_all(exp.Table):
            if not table.this:
                continue

            schema = table.db
            if schema or schema.lower() != self.lancedb_client.dataset_name:
                load_tables[table.name] = table.name

        if load_tables:
            self._create_views_for_tables(load_tables)

        with super().execute_query(query, *args, **kwargs) as cursor:
            yield cursor

    def _create_views_for_tables(self, tables: dict[str, str]) -> None:
        """Add the required tables as views to the duckdb in memory instance"""
        tables_with_data = (
            self.lancedb_client.schema.dlt_table_names()
            + self.lancedb_client.schema.data_table_names(seen_data_only=True)
        )
        for table_name in tables.keys():
            view_name = tables[table_name]

            if table_name not in tables_with_data:
                continue

            table_schema = self.lancedb_client.schema.get_table(table_name)
            table_schema["table_format"] = "lance"
            self._create_view(view_name, table_schema)

    def _create_view(self, view_name: str, table_schema: PreparedTableSchema) -> None:
        table_name = table_schema["name"]
        table_format = table_schema.get("table_format")
        if table_format != "lance":
            raise NotImplementedError(
                f"Can't create view for other formats than `.lance`. Received: {table_format}"
            )

        from_statement = f"{self.lance_uri}/{table_name}.lance"
        create_view_sql = f'CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM "{from_statement}"'
        self._conn.execute(create_view_sql)
