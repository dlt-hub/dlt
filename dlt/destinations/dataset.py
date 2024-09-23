from typing import Any, Generator, List, Tuple, Optional

from contextlib import contextmanager
from dlt.common.destination.reference import (
    SupportsReadableRelation,
    SupportsReadableDataset,
)

from dlt.destinations.typing import DataFrame, ArrowTable
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.destinations.sql_client import SqlClientBase
from dlt.common.schema import Schema


class ReadableDBAPIRelation(SupportsReadableRelation):
    def __init__(
        self,
        *,
        client: SqlClientBase[Any],
        table_name: str = None,
        query: str = None,
        schema: Schema = None,
    ) -> None:
        """Create a lazy evaluated relation to for the dataset of a destination"""
        self.client = client
        self.schema = schema
        self.query = query
        self.table_name = table_name
        self.schema_columns = {}

        # prepare query for table relation
        if self.table_name:
            table_name = client.make_qualified_table_name(self.table_name)
            self.query = f"SELECT * FROM {table_name}"
            self.schema_columns = self.schema.tables[self.table_name]["columns"]

    @contextmanager
    def cursor(self) -> Generator[SupportsReadableRelation, Any, Any]:
        """Gets a DBApiCursor for the current relation"""
        with self.client as client:
            with client.execute_query(self.query) as cursor:
                cursor.schema_columns = self.schema_columns
                yield cursor

    def df(self, chunk_size: int = None) -> Optional[DataFrame]:
        """Get first batch of table as dataframe"""
        with self.cursor() as cursor:
            return cursor.df(chunk_size=chunk_size)

    def arrow(self, chunk_size: int = None) -> Optional[ArrowTable]:
        """Get first batch of table as arrow table"""
        with self.cursor() as cursor:
            return cursor.arrow(chunk_size=chunk_size)

    def iter_df(self, chunk_size: int) -> Generator[DataFrame, None, None]:
        """iterates over the whole table in dataframes of the given chunk_size"""
        with self.cursor() as cursor:
            yield from cursor.iter_df(chunk_size=chunk_size)

    def iter_arrow(self, chunk_size: int) -> Generator[ArrowTable, None, None]:
        """iterates over the whole table in arrow tables of the given chunk_size"""
        with self.cursor() as cursor:
            yield from cursor.iter_arrow(chunk_size=chunk_size)

    def fetchall(self) -> List[Tuple[Any, ...]]:
        """does a dbapi fetch all"""
        with self.cursor() as cursor:
            return cursor.fetchall()

    def fetchmany(self, chunk_size: int) -> List[Tuple[Any, ...]]:
        """does a dbapi fetchmany with a given chunk size"""
        with self.cursor() as cursor:
            return cursor.fetchmany(chunk_size)

    def iter_fetchmany(self, chunk_size: int) -> Generator[List[Tuple[Any, ...]], Any, Any]:
        with self.cursor() as cursor:
            yield from cursor.iter_fetchmany(
                chunk_size=chunk_size,
            )

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        with self.cursor() as cursor:
            return cursor.fetchone()


class ReadableDBAPIDataset(SupportsReadableDataset):
    """Access to dataframes and arrowtables in the destination dataset via dbapi"""

    def __init__(self, client: SqlClientBase[Any], schema: Schema) -> None:
        self.client = client
        self.schema = schema

    def query(self, query: str) -> SupportsReadableRelation:
        return ReadableDBAPIRelation(client=self.client, query=query, schema=self.schema)

    def __getitem__(self, table_name: str) -> SupportsReadableRelation:
        """access of table via dict notation"""
        return ReadableDBAPIRelation(client=self.client, table_name=table_name, schema=self.schema)

    def __getattr__(self, table_name: str) -> SupportsReadableRelation:
        """access of table via property notation"""
        return self[table_name]
