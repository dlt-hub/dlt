from typing import Any, Generator, List, Tuple, Optional

from contextlib import contextmanager
from dlt.common.destination.reference import (
    WithReadableRelations,
    SupportsReadableRelation,
    SupportsReadableDataset,
)

from dlt.destinations.typing import DataFrame, ArrowTable
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.schema import Schema


class ReadableRelation(SupportsReadableRelation):
    def __init__(
        self,
        *,
        client: WithReadableRelations,
        table: str = None,
        query: str = None,
        columns: TTableSchemaColumns = None
    ) -> None:
        """Create a lazy evaluated relation to for the dataset of a destination"""
        self.client = client
        self.query = query
        self.table = table
        self.columns = columns

    @contextmanager
    def cursor(self) -> Generator[SupportsReadableRelation, Any, Any]:
        """Gets a DBApiCursor for the current relation"""
        if self.table:
            with self.client.table_relation(table=self.table, columns=self.columns) as cursor:
                yield cursor
        elif self.query:
            with self.client.query_relation(query=self.query) as cursor:
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


class ReadableDataset(SupportsReadableDataset):
    """Access to dataframes and arrowtables in the destination dataset"""

    def __init__(self, client: WithReadableRelations, schema: Schema) -> None:
        self.client = client
        self.schema = schema

    def query(self, query: str) -> SupportsReadableRelation:
        return ReadableRelation(client=self.client, query=query)

    def __getitem__(self, table: str) -> SupportsReadableRelation:
        """access of table via dict notation"""
        table_columns = self.schema.tables[table]["columns"]
        return ReadableRelation(client=self.client, table=table, columns=table_columns)

    def __getattr__(self, table: str) -> SupportsReadableRelation:
        """access of table via property notation"""
        return self[table]
