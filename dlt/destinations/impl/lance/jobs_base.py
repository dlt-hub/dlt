from typing import List, Optional

from dlt.common.destination.client import RunnableLoadJob
from dlt.common.schema.typing import TTableSchema
from dlt.destinations.impl.lance.lance_adapter import REMOVE_ORPHANS_HINT
from dlt.destinations.impl.lance.utils import build_orphan_scope_filter
from dlt.destinations.job_impl import ReferenceFollowupJobRequest
from dlt.destinations.sql_jobs import SqlMergeFollowupJob


class LanceReferenceJob(RunnableLoadJob):
    """Runs once per table, over every job file the reference file lists."""

    def __init__(self, file_path: str, table_schema: TTableSchema) -> None:
        super().__init__(file_path)
        self._table_schema: TTableSchema = table_schema
        self.file_paths: List[str] = ReferenceFollowupJobRequest.resolve_references(file_path)

    @property
    def _dataset_name(self) -> str:
        return self._job_client.dataset_name  # type: ignore[attr-defined,no-any-return]

    @property
    def merge_key(self) -> str:
        """Returns the column the merge matches on, deterministic for root and nested tables."""
        return SqlMergeFollowupJob.get_row_key_col(
            [self._load_table], self._load_table, self._dataset_name, self._dataset_name
        )

    def orphan_scope_filter(self) -> Optional[str]:
        """Returns the filter bounding orphan deletion to the documents of `file_paths`, `None`
        when the table keeps its orphans."""
        if not self._load_table[REMOVE_ORPHANS_HINT]:  # type: ignore[literal-required]
            return None
        return build_orphan_scope_filter(self._load_table, self.file_paths, self._dataset_name)
