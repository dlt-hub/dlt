from typing import cast, Any, TYPE_CHECKING, Generator

from contextlib import contextmanager


from dlt.common.destination.reference import SupportsDataAccess
from dlt.destinations.job_client_impl import SqlJobClientBase
from dlt.destinations.fs_client import FSClientBase
from dlt.destinations.typing import DataFrame, ArrowTable


class Dataset:
    """Access to dataframes and arrowtables in the destination dataset"""

    def __init__(self, pipeline: Any, table_name: str = None) -> None:
        from dlt.pipeline import Pipeline

        self.pipeline: Pipeline = cast(Pipeline, pipeline)
        self.table_name = table_name

    @contextmanager
    def _client(self) -> Generator[SupportsDataAccess, None, None]:
        """Get SupportsDataAccess destination object"""
        client = self.pipeline.destination_client()

        if isinstance(client, SqlJobClientBase):
            with client.sql_client as sql_client:
                yield sql_client
            return

        if isinstance(client, FSClientBase):
            yield client
            return

        raise Exception("Destination does not support data access")

    def df(self, *, sql: str = None, table: str = None, batch_size: int = 1000) -> DataFrame:
        """Get first batch of table as dataframe"""
        return next(self.iter_df(sql=sql, table=table, batch_size=batch_size))

    def arrow(self, *, sql: str = None, table: str = None, batch_size: int = 1000) -> ArrowTable:
        """Get first batch of table as arrow table"""
        return next(self.iter_arrow(sql=sql, table=table, batch_size=batch_size))

    def iter_df(
        self, *, sql: str = None, table: str = None, batch_size: int = 1000
    ) -> Generator[DataFrame, None, None]:
        """iterates over the whole table in dataframes of the given batch_size, batch_size of -1 will return the full table in the first batch"""
        # if no table is given, take the bound table
        table = self.table_name or table
        with self._client() as data_access:
            yield from data_access.iter_df(sql=sql, table=table, batch_size=batch_size)

    def iter_arrow(
        self, *, sql: str = None, table: str = None, batch_size: int = 1000
    ) -> Generator[ArrowTable, None, None]:
        """iterates over the whole table in arrow tables of the given batch_size, batch_size of -1 will return the full table in the first batch"""
        # if no table is given, take the bound table
        table = self.table_name or table
        with self._client() as data_access:
            yield from data_access.iter_arrow(sql=sql, table=table, batch_size=batch_size)
