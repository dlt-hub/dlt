
from typing import IO, Dict, Any, Iterator, Sequence
import sqlalchemy as sa

from dlt.common.destination.client import (
    LoadJob,
    PreparedTableSchema
)
from dlt.common.destination.client import (
    RunnableLoadJob,
    HasFollowupJobs
)
from dlt.common.storages import FileStorage
from dlt.common.json import json, PY_DATETIME_DECODERS
from dlt.destinations.impl.sqlalchemy.sqlalchemy_job_client import SqlalchemyJobClient

class StarrocksStreamLoadJob(RunnableLoadJob, HasFollowupJobs):
    def __init__(self, file_path: str, table: sa.Table) -> None:
        super().__init__(file_path)
        self._job_client: "StarrocksJobClient" = None
        self.table = table

    def _open_load_file(self) -> IO[bytes]:
        return FileStorage.open_zipsafe_ro(self._file_path, "rb")

    def _iter_data_items(self) -> Iterator[Dict[str, Any]]:
        all_cols = {col.name: None for col in self.table.columns}
        with FileStorage.open_zipsafe_ro(self._file_path, "rb") as f:
            for line in f:
                # Decode date/time to py datetime objects. Some drivers have issues with pendulum objects
                for item in json.typed_loadb(line, decoders=PY_DATETIME_DECODERS):
                    # Fill any missing columns in item with None. Bulk insert fails when items have different keys
                    if item.keys() != all_cols.keys():
                        yield {**all_cols, **item}
                    else:
                        yield item

    def _iter_data_item_chunks(self) -> Iterator[Sequence[Dict[str, Any]]]:
        max_rows = self._job_client.capabilities.max_rows_per_insert or math.inf
        # Limit by max query length should not be needed,
        # bulk insert generates an INSERT template with a single VALUES tuple of placeholders
        # If any dialects don't do that we need to check the str length of the query
        # TODO: Max params may not be needed. Limits only apply to placeholders in sql string (mysql/sqlite)
        max_params = self._job_client.capabilities.max_query_parameters or math.inf
        chunk: List[Dict[str, Any]] = []
        params_count = 0
        for item in self._iter_data_items():
            if len(chunk) + 1 == max_rows or params_count + len(item) > max_params:
                # Rotate chunk
                yield chunk
                chunk = []
                params_count = 0
            params_count += len(item)
            chunk.append(item)

        if chunk:
            yield chunk

    def run(self) -> None:
        _sql_client = self._job_client.sql_client
        # Copy the table to the current dataset (i.e. staging) if needed
        # This is a no-op if the table is already in the correct schema
        table = self.table.to_metadata(
            self.table.metadata, schema=_sql_client.dataset_name  # type: ignore[attr-defined]
        )

        with _sql_client.begin_transaction():
            for chunk in self._iter_data_item_chunks():
                print(table, chunk)
                # _sql_client.execute_sql(table.insert(), chunk)

class StarrocksJobClient(SqlalchemyJobClient):
    def create_load_job(
        self, table: PreparedTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        table_obj = self._to_table_object(table)
        job = StarrocksStreamLoadJob(file_path, table_obj)
        
        if job is not None:
            return job
        
        job = super().create_load_job(table, file_path, load_id, restore)
        return job
