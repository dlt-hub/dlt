from typing import Any, Generator, List, Tuple, Optional

from contextlib import contextmanager
from dlt.destinations.job_client_impl import SqlJobClientBase
from dlt.destinations.fs_client import FSClientBase

from dlt.common.destination.reference import (
    SupportsRelationshipAccess,
    SupportsReadRelation,
    JobClientBase,
    SupportsReadDataset,
)

from dlt.common.typing import DataFrame, ArrowTable


class Relation(SupportsReadRelation):
    def __init__(
        self,
        *,
        job_client: JobClientBase,
        table: str = None,
        sql: str = None,
        prepare_tables: List[str] = None,
    ) -> None:
        """Create a lazy evaluated relation to for the dataset of a destination"""
        self.job_client = job_client
        self.prepare_tables = prepare_tables
        self.sql = sql
        self.table = table

    @contextmanager
    def _client(self) -> Generator[SupportsRelationshipAccess, Any, Any]:
        if isinstance(self.job_client, SqlJobClientBase):
            with self.job_client.sql_client as sql_client:
                yield sql_client
            return

        if isinstance(self.job_client, FSClientBase):
            yield self.job_client
            return

        raise Exception(
            f"Destination {self.job_client.config.destination_type} does not support data access"
            " via dataset."
        )

    @contextmanager
    def cursor(self) -> Generator[SupportsReadRelation, Any, Any]:
        """Gets a DBApiCursor for the current relation"""
        with self._client() as client:
            with client.cursor_for_relation(
                sql=self.sql, table=self.table, prepare_tables=self.prepare_tables
            ) as cursor:
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
        """iterates over the whole table in dataframes of the given chunk_size, chunk_size of -1 will return the full table in the first batch"""
        with self.cursor() as cursor:
            yield from cursor.iter_df(
                chunk_size=chunk_size,
            )

    def iter_arrow(
        self,
        chunk_size: int,
    ) -> Generator[ArrowTable, None, None]:
        """iterates over the whole table in arrow tables of the given chunk_size, chunk_size of -1 will return the full table in the first batch"""
        with self.cursor() as cursor:
            yield from cursor.iter_arrow(
                chunk_size=chunk_size,
            )

    def fetchall(self) -> List[Tuple[Any, ...]]:
        with self.cursor() as cursor:
            return cursor.fetchall()

    def fetchmany(self, chunk_size: int) -> List[Tuple[Any, ...]]:
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


class Dataset(SupportsReadDataset):
    """Access to dataframes and arrowtables in the destination dataset"""

    def __init__(self, job_client: JobClientBase) -> None:
        self.job_client = job_client

    def sql(self, sql: str, prepare_tables: List[str] = None) -> SupportsReadRelation:
        return Relation(job_client=self.job_client, sql=sql, prepare_tables=prepare_tables)

    def __getitem__(self, table: str) -> SupportsReadRelation:
        """access of table via dict notation"""
        return Relation(job_client=self.job_client, table=table)

    def __getattr__(self, table: str) -> SupportsReadRelation:
        """access of table via property notation"""
        return Relation(job_client=self.job_client, table=table)
