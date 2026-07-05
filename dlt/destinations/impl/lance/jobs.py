from typing import TYPE_CHECKING, List

from dlt.common import json
from dlt.common.destination.client import HasFollowupJobs, RunnableLoadJob
from dlt.common.libs.pyarrow import pyarrow as pa, get_local_dataset_reader
from dlt.common.schema.typing import TTableSchema
from dlt.common.schema.utils import is_nested_table
from dlt.common.storages.load_package import (
    ParsedLoadJobFileName,
    commit_load_package_state,
    destination_state,
)
from dlt.destinations.impl.lance.lance_adapter import REMOVE_ORPHANS_HINT
from dlt.destinations.impl.lance.utils import (
    get_canonical_vector_database_doc_id_merge_key,
    create_in_filter,
)
from dlt.destinations.job_impl import ReferenceFollowupJobRequest
from dlt.destinations.sql_jobs import SqlMergeFollowupJob

if TYPE_CHECKING:
    from lance import LanceDataset, LanceOperation
    from lance.fragment import FragmentMetadata

    from dlt.destinations.impl.lance.lance_client import LanceClient

LANCE_FRAGMENTS_STATE_KEY = "lance_fragments"


def _get_file_reader(file_paths: List[str]) -> pa.RecordBatchReader:
    # native reader: batches are pulled in the writer without per-batch GIL round trips
    return get_local_dataset_reader(file_paths)


class LanceLoadJob(RunnableLoadJob, HasFollowupJobs):
    """Writes data files as uncommitted lance fragments, `LanceCommitLoadJob` commits them."""

    def __init__(
        self,
        file_path: str,
        table_schema: TTableSchema,
    ) -> None:
        super().__init__(file_path)
        self._job_client: LanceClient = None
        self._table_schema: TTableSchema = table_schema

    def run(self) -> None:
        from lance.fragment import write_fragments

        table_name: str = self._table_schema["name"]
        ds = self._job_client.open_lance_dataset(
            table_name, branch_name=self._job_client.config.branch_name
        )
        records = self._job_client.prepare_records(_get_file_reader([self._file_path]), ds.schema)
        # fragments are not visible until committed, concurrent jobs do not conflict
        fragments = write_fragments(records, ds)
        table_state = (
            destination_state().setdefault(LANCE_FRAGMENTS_STATE_KEY, {}).setdefault(table_name, {})
        )
        # keyed by job id so a retried job replaces its own fragments
        table_state[self._parsed_file_name.job_id()] = [f.to_json() for f in fragments]
        commit_load_package_state()


class LanceCommitLoadJob(RunnableLoadJob):
    """Completes a table in a single lance commit: appends or overwrites the fragments
    written by `LanceLoadJob`, or merges all job files at once."""

    def __init__(self, file_path: str, table_schema: TTableSchema, restore: bool = False) -> None:
        super().__init__(file_path)
        self._job_client: LanceClient = None
        self._table_schema: TTableSchema = table_schema
        self._restore = restore
        self.file_paths: List[str] = ReferenceFollowupJobRequest.resolve_references(file_path)

    def run(self) -> None:
        from lance import LanceDataset, LanceOperation

        table_name: str = self._table_schema["name"]
        write_disposition = self._load_table.get("write_disposition", "append")
        ds = self._job_client.open_lance_dataset(
            table_name, branch_name=self._job_client.config.branch_name
        )

        if write_disposition == "merge":
            self._merge(ds)
            return

        fragments = self._collect_fragments(table_name)
        if write_disposition == "replace":
            # a single Overwrite commit replaces the data, no truncate round trip.
            # re-running it on job resume yields the same result
            operation: "LanceOperation.BaseOperation" = LanceOperation.Overwrite(
                ds.schema, fragments
            )
        elif fragments:
            # only a re-run job may have already committed: a crash or transient error
            # between a successful commit and completing the job would duplicate the append
            is_rerun = self._restore or self._parsed_file_name.retry_count > 0
            if is_rerun and self._fragments_committed(ds, fragments):
                return
            operation = LanceOperation.Append(fragments)
        else:
            return
        LanceDataset.commit(ds, operation, read_version=ds.version)

    @staticmethod
    def _fragments_committed(ds: "LanceDataset", fragments: List["FragmentMetadata"]) -> bool:
        pending_file = fragments[0].files[0].path
        return any(
            pending_file == f.path for frag in ds.get_fragments() for f in frag.metadata.files
        )

    def _collect_fragments(self, table_name: str) -> List["FragmentMetadata"]:
        from lance.fragment import FragmentMetadata

        table_state = destination_state().get(LANCE_FRAGMENTS_STATE_KEY, {}).get(table_name, {})
        job_ids = [ParsedLoadJobFileName.parse(p).job_id() for p in self.file_paths]
        return [
            FragmentMetadata.from_json(json.dumps(fragment))
            for job_id in job_ids
            for fragment in table_state.get(job_id, [])
        ]

    def _merge(self, ds: "LanceDataset") -> None:
        # use deterministic and unique id as a merge column (to perform classical upsert)
        # NOTE: upsert strategy generates deterministic row_key both for root and nested tables
        merge_key = SqlMergeFollowupJob.get_row_key_col(
            [self._load_table],
            self._load_table,
            self._job_client.dataset_name,
            self._job_client.dataset_name,
        )
        when_not_matched_by_source_delete_expr: str = None
        if self._load_table[REMOVE_ORPHANS_HINT]:  # type: ignore[literal-required]
            when_not_matched_by_source_delete_expr = self._build_remove_orphans_scope_expr()

        records = self._job_client.prepare_records(_get_file_reader(self.file_paths), ds.schema)
        self._job_client._write_records(
            ds,
            records,
            write_disposition="merge",
            merge_key=merge_key,
            when_not_matched_by_source_delete_expr=when_not_matched_by_source_delete_expr,
        )

    def _build_remove_orphans_scope_expr(self) -> str:
        """Builds SQL filter for `when_not_matched_by_source_delete` clause that scopes orphan deletion to current load.

        This filter is relevant for incremental loads. It ensures only target rows whose key appears
        in this load are considered for deletion, leaving rows from prior loads untouched.
        """

        key_col = (
            SqlMergeFollowupJob.get_root_key_col(
                [self._load_table],
                self._load_table,
                self._job_client.dataset_name,
                self._job_client.dataset_name,
            )
            if is_nested_table(self._load_table)
            else get_canonical_vector_database_doc_id_merge_key(self._load_table)
        )
        # unfortunately we need to load data into memory here before the write, but at least we can
        # scope it to just the key column
        keys = pa.concat_tables(
            pa.parquet.read_table(path, columns=[key_col]) for path in self.file_paths
        )[key_col]
        return create_in_filter(key_col, keys)
