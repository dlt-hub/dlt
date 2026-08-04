from typing import TYPE_CHECKING

from dlt.common.destination.client import (
    RunnableLoadJob,
    HasFollowupJobs,
)
from dlt.common.destination.utils import resolve_merge_strategy
from dlt.common.libs.pyarrow import pyarrow as pa
from dlt.common.schema.typing import (
    TWriteDisposition,
    TTableSchema,
)
from dlt.common.schema.utils import is_nested_table
from dlt.common.storages import ParsedLoadJobFileName
from dlt.destinations.impl.lance.utils import (
    create_in_filter,
    get_canonical_vector_database_doc_id_merge_key,
)
from dlt.destinations.impl.lancedb.schema import (
    TTableLineage,
    TableJob,
)
from dlt.destinations.job_impl import ReferenceFollowupJobRequest
from dlt.destinations.sql_jobs import SqlMergeFollowupJob

if TYPE_CHECKING:
    from dlt.destinations.impl.lancedb.lancedb_client import LanceDBClient


class LanceDBLoadJob(RunnableLoadJob, HasFollowupJobs):
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

        merge_key: str = None
        merge_strategy = None
        if write_disposition == "merge":
            merge_key = SqlMergeFollowupJob.get_row_key_col(
                [self._load_table],
                self._load_table,
                self._job_client.dataset_name,
                self._job_client.dataset_name,
            )
            merge_strategy = resolve_merge_strategy(
                {self._load_table["name"]: self._load_table}, self._load_table
            )

        with open(self._file_path, mode="rb") as f:
            arrow_table: pa.Table = pa.parquet.read_table(f)

        self._job_client.write_records(
            arrow_table,
            self._table_schema["name"],
            write_disposition=write_disposition,
            merge_key=merge_key,
            merge_strategy=merge_strategy,
        )


class LanceDBRemoveOrphansJob(RunnableLoadJob):
    def __init__(
        self,
        file_path: str,
    ) -> None:
        super().__init__(file_path)
        self._job_client: "LanceDBClient" = None
        self.references = ReferenceFollowupJobRequest.resolve_references(file_path)

    def run(self) -> None:
        table_lineage: TTableLineage = [
            TableJob(
                table_schema=self._job_client.prepare_load_table(
                    ParsedLoadJobFileName.parse(file_path_).table_name
                ),
                table_name=ParsedLoadJobFileName.parse(file_path_).table_name,
                file_path=file_path_,
            )
            for file_path_ in self.references
        ]

        for job in table_lineage:
            target_is_root_table = not is_nested_table(job.table_schema)
            with open(job.file_path, mode="rb") as f:
                payload_arrow_table: pa.Table = pa.parquet.read_table(f)

            if target_is_root_table:
                canonical_doc_id_field = get_canonical_vector_database_doc_id_merge_key(
                    job.table_schema
                )
                # delete all records with load id different than load id of payload_arrow_table
                # that have docs ids in payload_arrow_table (orphaned rows)
                delete_condition = create_in_filter(
                    canonical_doc_id_field, payload_arrow_table[canonical_doc_id_field]
                )
                # TODO: raise if dlt_load_id not present in payload_arrow_table. most probably
                #   arrow tables are used but normalizer skips _dlt_load_id
                dlt_load_id = self._schema.data_item_normalizer.c_dlt_load_id  # type: ignore[attr-defined]
                merge_key = dlt_load_id

            else:
                dlt_id = SqlMergeFollowupJob.get_row_key_col(
                    [job.table_schema],
                    job.table_schema,
                    self._job_client.dataset_name,
                    self._job_client.dataset_name,
                )
                dlt_root_id = SqlMergeFollowupJob.get_root_key_col(
                    [job.table_schema],
                    job.table_schema,
                    self._job_client.dataset_name,
                    self._job_client.dataset_name,
                )
                # delete all records with dlt id not in payload_arrow_table
                # that have root key id in payload_arrow_table (orphaned rows)
                delete_condition = create_in_filter(
                    dlt_root_id,
                    payload_arrow_table[dlt_root_id],
                )
                merge_key = dlt_id

            self._job_client.write_records(
                payload_arrow_table,
                job.table_name,
                write_disposition="merge",
                merge_key=merge_key,
                remove_orphans=True,
                delete_condition=delete_condition,
            )
