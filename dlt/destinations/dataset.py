from typing import Any, Generator

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
        query: str,
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
        self.iter_fetchmany = self._wrap_iter("iter_fetchmany")  # type: ignore

    @contextmanager
    def cursor(self) -> Generator[SupportsReadableRelation, Any, Any]:
        """Gets a DBApiCursor for the current relation"""
        with self.client as client:
            with client.execute_query(self.query) as cursor:
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

    def __init__(self, client: SqlClientBase[Any], schema: Schema) -> None:
        self.client = client
        self.schema = schema

    def query(
        self, query: str, schema_columns: TTableSchemaColumns = None
    ) -> SupportsReadableRelation:
        schema_columns = schema_columns or {}
        return ReadableDBAPIRelation(client=self.client, query=query, schema_columns=schema_columns)  # type: ignore[abstract]

    def table(self, table_name: str) -> SupportsReadableRelation:
        # prepare query for table relation
        table_name = self.client.make_qualified_table_name(table_name)
        query = f"SELECT * FROM {table_name}"
        schema_columns = self.schema.tables.get(table_name, {}).get("columns", {})
        return self.query(query, schema_columns)

    def __getitem__(self, table_name: str) -> SupportsReadableRelation:
        """access of table via dict notation"""
        return self.table(table_name)

    def __getattr__(self, table_name: str) -> SupportsReadableRelation:
        """access of table via property notation"""
        return self.table(table_name)
