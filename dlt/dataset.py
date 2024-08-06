from typing import cast, Any, TYPE_CHECKING, Generator, List

from contextlib import contextmanager


from dlt.common.destination.reference import SupportsDataAccess

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
    def _client(self) -> Generator[SupportsDataAccess, None, None]:
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

    def df(
        self,
        *,
        batch_size: int = 1000,
    ) -> DataFrame:
        """Get first batch of table as dataframe"""
        return next(
            self.iter_df(
                batch_size=batch_size,
            )
        )

    def arrow(
        self,
        *,
        batch_size: int = 1000,
    ) -> ArrowTable:
        """Get first batch of table as arrow table"""
        return next(
            self.iter_arrow(
                batch_size=batch_size,
            )
        )

    def iter_df(
        self,
        *,
        batch_size: int = 1000,
    ) -> Generator[DataFrame, None, None]:
        """iterates over the whole table in dataframes of the given batch_size, batch_size of -1 will return the full table in the first batch"""
        # if no table is given, take the bound table
        with self._client() as data_access:
            yield from data_access.iter_df(
                sql=self.sql,
                table=self.table,
                batch_size=batch_size,
                prepare_tables=self.prepare_tables,
            )

    def iter_arrow(
        self,
        *,
        batch_size: int = 1000,
    ) -> Generator[ArrowTable, None, None]:
        """iterates over the whole table in arrow tables of the given batch_size, batch_size of -1 will return the full table in the first batch"""
        # if no table is given, take the bound table
        with self._client() as data_access:
            yield from data_access.iter_arrow(
                sql=self.sql,
                table=self.table,
                batch_size=batch_size,
                prepare_tables=self.prepare_tables,
            )


class Dataset:
    """Access to dataframes and arrowtables in the destination dataset"""

    def __init__(self, pipeline: Any) -> None:
        from dlt.pipeline import Pipeline

        self.pipeline: Pipeline = cast(Pipeline, pipeline)

    def sql(self, sql: str, prepare_tables: List[str] = None) -> Relation:
        return Relation(pipeline=self.pipeline, sql=sql, prepare_tables=prepare_tables)

    def __getitem__(self, table: str) -> Relation:
        return Relation(pipeline=self.pipeline, table=table)

    def __getattr__(self, table: str) -> Relation:
        return Relation(pipeline=self.pipeline, table=table)
