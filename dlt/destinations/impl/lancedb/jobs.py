from typing import TYPE_CHECKING

from dlt.common.destination.client import HasFollowupJobs, RunnableLoadJob
from dlt.common.destination.utils import resolve_merge_strategy
from dlt.common.libs.pyarrow import get_local_dataset_reader
from dlt.common.schema.typing import TTableSchema, TWriteDisposition
from dlt.destinations.impl.lance.jobs_base import LanceReferenceJob

if TYPE_CHECKING:
    from dlt.destinations.impl.lancedb.lancedb_client import LanceDBClient


class LanceDBLoadJob(RunnableLoadJob, HasFollowupJobs):
    """Adds one job file to a table, for the dispositions that do not merge."""

    def __init__(
        self,
        file_path: str,
        table_schema: TTableSchema,
    ) -> None:
        super().__init__(file_path)
        self._job_client: "LanceDBClient" = None
        self._table_schema: TTableSchema = table_schema

    def run(self) -> None:
        write_disposition: TWriteDisposition = self._load_table.get("write_disposition", "append")
        self._job_client.write_records(
            get_local_dataset_reader([self._file_path]),
            self._table_schema["name"],
            write_disposition=write_disposition,
        )


class LanceDBMergeJob(LanceReferenceJob):
    """Updates, inserts and removes the orphans of a table in a single `merge_insert`."""

    def __init__(
        self,
        file_path: str,
        table_schema: TTableSchema,
    ) -> None:
        super().__init__(file_path, table_schema)
        self._job_client: "LanceDBClient" = None

    def run(self) -> None:
        merge_strategy = resolve_merge_strategy(
            {self._load_table["name"]: self._load_table}, self._load_table
        )
        # `insert-only` leaves what is already in the table alone, orphans included
        delete_expr = None if merge_strategy == "insert-only" else self.orphan_scope_filter()
        self._job_client.write_records(
            get_local_dataset_reader(self.file_paths),
            self._table_schema["name"],
            write_disposition="merge",
            merge_key=self.merge_key,
            merge_strategy=merge_strategy,
            when_not_matched_by_source_delete_expr=delete_expr,
        )
