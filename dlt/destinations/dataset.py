from typing import Any, Generator, List, Tuple, Optional

from contextlib import contextmanager
from dlt.common.destination.reference import (
    WithReadableRelations,
    SupportsReadableRelation,
    SupportsReadableDataset,
)

from dlt.destinations.typing import DataFrame, ArrowTable


class ReadableRelation(SupportsReadableRelation):
    def __init__(
        self,
        *,
        client: WithReadableRelations,
        table: str = None,
        sql: str = None,
    ) -> None:
        """Create a lazy evaluated relation to for the dataset of a destination"""
        self.client = client
        self.sql = sql
        self.table = table

    @contextmanager
    def cursor(self) -> Generator[SupportsReadableRelation, Any, Any]:
        """Gets a DBApiCursor for the current relation"""
        with self.client.get_readable_relation(sql=self.sql, table=self.table) as cursor:
            yield cursor

    def df(
        self,
        chunk_size: int = None,
    ) -> Optional[DataFrame]:
        """Get first batch of table as dataframe"""
        with self.cursor() as cursor:
            return cursor.df(chunk_size=chunk_size)

    def arrow(
        self,
        chunk_size: int = None,
    ) -> Optional[ArrowTable]:
        """Get first batch of table as arrow table"""
        with self.cursor() as cursor:
            return cursor.arrow(chunk_size=chunk_size)

    def iter_df(
        self,
        chunk_size: int,
    ) -> Generator[DataFrame, None, None]:
        """iterates over the whole table in dataframes of the given chunk_size"""
        with self.cursor() as cursor:
            yield from cursor.iter_df(
                chunk_size=chunk_size,
            )

    def iter_arrow(
        self,
        chunk_size: int,
    ) -> Generator[ArrowTable, None, None]:
        """iterates over the whole table in arrow tables of the given chunk_size"""
        with self.cursor() as cursor:
            yield from cursor.iter_arrow(
                chunk_size=chunk_size,
            )

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

    def __init__(self, client: WithReadableRelations) -> None:
        self.client = client

    def query(self, sql: str) -> SupportsReadableRelation:
        return ReadableRelation(client=self.client, sql=sql)

    def __getitem__(self, table: str) -> SupportsReadableRelation:
        """access of table via dict notation"""
        return ReadableRelation(client=self.client, table=table)

    def __getattr__(self, table: str) -> SupportsReadableRelation:
        """access of table via property notation"""
        return ReadableRelation(client=self.client, table=table)
