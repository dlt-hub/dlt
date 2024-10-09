from typing import Any, Generator, AnyStr, Optional

from contextlib import contextmanager
from dlt.common.destination.reference import (
    SupportsReadableRelation,
    SupportsReadableDataset,
)

from dlt.common.schema.typing import TTableSchemaColumns
from dlt.destinations.sql_client import SqlClientBase
from dlt.common.schema import Schema


class ReadableDBAPIRelation(SupportsReadableRelation):
    def __init__(
        self,
        *,
        client: SqlClientBase[Any],
        query: Any,
        schema_columns: TTableSchemaColumns = None,
    ) -> None:
        """Create a lazy evaluated relation to for the dataset of a destination"""
        self.client = client
        self.schema_columns = schema_columns
        self.query = query

        # wire protocol functions
        self.df = self._wrap_func("df")  # type: ignore
        self.arrow = self._wrap_func("arrow")  # type: ignore
        self.fetchall = self._wrap_func("fetchall")  # type: ignore
        self.fetchmany = self._wrap_func("fetchmany")  # type: ignore
        self.fetchone = self._wrap_func("fetchone")  # type: ignore

        self.iter_df = self._wrap_iter("iter_df")  # type: ignore
        self.iter_arrow = self._wrap_iter("iter_arrow")  # type: ignore
        self.iter_fetch = self._wrap_iter("iter_fetch")  # type: ignore

    @contextmanager
    def cursor(self) -> Generator[SupportsReadableRelation, Any, Any]:
        """Gets a DBApiCursor for the current relation"""
        with self.client as client:
            # this hacky code is needed for mssql to disable autocommit, read iterators
            # will not work otherwise. in the future we should be able to create a readony
            # client which will do this automatically
            if hasattr(self.client, "_conn") and hasattr(self.client._conn, "autocommit"):
                self.client._conn.autocommit = False
            with client.execute_query(self.query) as cursor:
                if self.schema_columns:
                    cursor.schema_columns = self.schema_columns
                yield cursor

    def _wrap_iter(self, func_name: str) -> Any:
        """wrap SupportsReadableRelation generators in cursor context"""

        def _wrap(*args: Any, **kwargs: Any) -> Any:
            with self.cursor() as cursor:
                yield from getattr(cursor, func_name)(*args, **kwargs)

        return _wrap

    def _wrap_func(self, func_name: str) -> Any:
        """wrap SupportsReadableRelation functions in cursor context"""

        def _wrap(*args: Any, **kwargs: Any) -> Any:
            with self.cursor() as cursor:
                return getattr(cursor, func_name)(*args, **kwargs)

        return _wrap


class ReadableDBAPIDataset(SupportsReadableDataset):
    """Access to dataframes and arrowtables in the destination dataset via dbapi"""

    def __init__(self, client: SqlClientBase[Any], schema: Optional[Schema]) -> None:
        self.client = client
        self.schema = schema

    def __call__(
        self, query: Any, schema_columns: TTableSchemaColumns = None
    ) -> ReadableDBAPIRelation:
        schema_columns = schema_columns or {}
        return ReadableDBAPIRelation(client=self.client, query=query, schema_columns=schema_columns)  # type: ignore[abstract]

    def table(self, table_name: str) -> SupportsReadableRelation:
        # prepare query for table relation
        schema_columns = (
            self.schema.tables.get(table_name, {}).get("columns", {}) if self.schema else {}
        )
        table_name = self.client.make_qualified_table_name(table_name)
        query = f"SELECT * FROM {table_name}"
        return self(query, schema_columns)

    def __getitem__(self, table_name: str) -> SupportsReadableRelation:
        """access of table via dict notation"""
        return self.table(table_name)

    def __getattr__(self, table_name: str) -> SupportsReadableRelation:
        """access of table via property notation"""
        return self.table(table_name)
