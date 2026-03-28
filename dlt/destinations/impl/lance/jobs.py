from typing import cast

import pyarrow as pa
import pyarrow.parquet as pq

from dlt.common.destination.client import RunnableLoadJob
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.schema.typing import (
    TWriteDisposition,
    TTableSchema,
)
from dlt.common.schema.utils import is_nested_table
from dlt.destinations.impl.lancedb.lancedb_adapter import NO_REMOVE_ORPHANS_HINT
from dlt.destinations.impl.lance.utils import (
    get_canonical_vector_database_doc_id_merge_key,
    create_in_filter,
)
from dlt.destinations.sql_jobs import SqlMergeFollowupJob


class LanceLoadJob(RunnableLoadJob):
    def __init__(
        self,
        file_path: str,
        table_schema: TTableSchema,
    ) -> None:
        from dlt.destinations.impl.lance.lance_client import LanceClient

        super().__init__(file_path)
        self._job_client: "LanceClient" = None
        self._table_schema: TTableSchema = table_schema

    def run(self) -> None:
        table_name: str = self._table_schema["name"]
        write_disposition: TWriteDisposition = cast(
            TWriteDisposition, self._load_table.get("write_disposition", "append")
        )

        merge_key: str = None
        when_not_matched_by_source_delete_expr: str = None
        if write_disposition == "merge":
            # use deterministic and unique id as a merge column (to perform classical upsert)
            # NOTE: upsert strategy generates deterministic row_key both for root and nested tables
            merge_key = SqlMergeFollowupJob.get_row_key_col(
                [self._load_table],
                self._load_table,
                self._job_client.dataset_name,
                self._job_client.dataset_name,
            )

            if self._should_remove_orphans(self._load_table):
                when_not_matched_by_source_delete_expr = self._build_remove_orphans_scope_expr()

        self._job_client.write_records(
            self._get_file_reader(self._file_path),
            table_name,
            write_disposition=write_disposition,
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
        keys = pq.read_table(self._file_path, columns=[key_col])[key_col]
        return create_in_filter(key_col, keys)

    @staticmethod
    def _should_remove_orphans(table: PreparedTableSchema) -> bool:
        return not table.get(NO_REMOVE_ORPHANS_HINT)

    @staticmethod
    def _get_file_reader(file_path: str) -> pa.RecordBatchReader:
        parquet_file = pq.ParquetFile(file_path)
        return pa.RecordBatchReader.from_batches(
            parquet_file.schema_arrow, parquet_file.iter_batches()
        )
