from typing import cast, Any, TYPE_CHECKING, Generator, List, Tuple, Optional

from contextlib import contextmanager

from dlt.common.destination.reference import SupportsRelationshipAccess, SupportsDataAccess

from dlt.common.typing import DataFrame, ArrowTable


class Relation:
    def __init__(
        self, *, pipeline: Any, table: str = None, sql: str = None, prepare_tables: List[str] = None
    ) -> None:
        """Create a lazy evaluated relation to for the dataset of a pipeline"""
        from dlt.pipeline import Pipeline

        self.pipeline: Pipeline = cast(Pipeline, pipeline)
        self.prepare_tables = prepare_tables
        self.sql = sql
        self.table = table

    @contextmanager
    def _client(self) -> Generator[SupportsRelationshipAccess, Any, Any]:
        from dlt.destinations.job_client_impl import SqlJobClientBase
        from dlt.destinations.fs_client import FSClientBase

        client = self.pipeline.destination_client()

        if isinstance(client, SqlJobClientBase):
            with client.sql_client as sql_client:
                yield sql_client
            return

        if isinstance(client, FSClientBase):
            yield client
            return

        raise Exception(
            f"Destination {client.config.destination_type} does not support data access via"
            " dataset."
        )

    @contextmanager
    def cursor(self) -> Generator[SupportsDataAccess, Any, Any]:
        """Gets a DBApiCursor for the current relation"""
        with self._client() as client:
            with client.cursor_for_relation(
                sql=self.sql, table=self.table, prepare_tables=self.prepare_tables
            ) as cursor:
                yield cursor

    def df(
        self,
        *,
        chunk_size: int = None,
    ) -> DataFrame:
        """Get first batch of table as dataframe"""
        with self.cursor() as cursor:
            return cursor.df(chunk_size=chunk_size)

    def arrow(
        self,
        *,
        chunk_size: int = None,
    ) -> ArrowTable:
        """Get first batch of table as arrow table"""
        with self.cursor() as cursor:
            return cursor.arrow(chunk_size=chunk_size)

    def iter_df(
        self,
        *,
        chunk_size: int,
    ) -> Generator[DataFrame, None, None]:
        """iterates over the whole table in dataframes of the given chunk_size, chunk_size of -1 will return the full table in the first batch"""
        with self.cursor() as cursor:
            yield from cursor.iter_df(
                chunk_size=chunk_size,
            )

    def iter_arrow(
        self,
        *,
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


class Dataset:
    """Access to dataframes and arrowtables in the destination dataset"""

    def __init__(self, pipeline: Any) -> None:
        from dlt.pipeline import Pipeline

        self.pipeline: Pipeline = cast(Pipeline, pipeline)

    def sql(self, sql: str, prepare_tables: List[str] = None) -> Relation:
        return Relation(pipeline=self.pipeline, sql=sql, prepare_tables=prepare_tables)

    def __getitem__(self, table: str) -> Relation:
        """access of table via dict notation"""
        return Relation(pipeline=self.pipeline, table=table)

    def __getattr__(self, table: str) -> Relation:
        """access of table via property notation"""
        return Relation(pipeline=self.pipeline, table=table)
