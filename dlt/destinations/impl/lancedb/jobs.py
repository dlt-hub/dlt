from typing import cast
import pyarrow as pa
import pyarrow.parquet as pq
from lancedb import DBConnection  # type: ignore

from dlt.common.destination.client import (
    RunnableLoadJob,
    HasFollowupJobs,
)
from dlt.common.schema.typing import (
    TWriteDisposition,
    TTableSchema,
)
from dlt.common.schema.utils import (
    get_columns_names_with_prop,
    is_nested_table,
)
from dlt.common.storages import ParsedLoadJobFileName
from dlt.destinations.impl.lancedb.lancedb_adapter import (
    VECTORIZE_HINT,
)
from dlt.destinations.impl.lancedb.schema import (
    TArrowSchema,
    TTableLineage,
    TableJob,
)
from dlt.destinations.impl.lancedb.utils import (
    EMPTY_STRING_PLACEHOLDER,
    fill_empty_source_column_values_with_placeholder,
    get_canonical_vector_database_doc_id_merge_key,
    create_in_filter,
    write_records,
)
from dlt.destinations.job_impl import ReferenceFollowupJobRequest
from dlt.destinations.sql_jobs import SqlMergeFollowupJob


class LanceDBLoadJob(RunnableLoadJob, HasFollowupJobs):
    arrow_schema: TArrowSchema

    def __init__(
        self,
        file_path: str,
        table_schema: TTableSchema,
    ) -> None:
        from dlt.destinations.impl.lancedb.lancedb_client import LanceDBClient

        super().__init__(file_path)
        self._job_client: "LanceDBClient" = None
        self._table_schema: TTableSchema = table_schema

    def run(self) -> None:
        db_client: DBConnection = self._job_client.db_client
        fq_table_name: str = self._job_client.make_qualified_table_name(self._table_schema["name"])
        write_disposition: TWriteDisposition = cast(
            TWriteDisposition, self._load_table.get("write_disposition", "append")
        )

        with open(self._file_path, mode="rb") as f:
            arrow_table: pa.Table = pq.read_table(f)

        # Replace empty strings with placeholder string if OpenAI is used.
        # https://github.com/lancedb/lancedb/issues/1577#issuecomment-2318104218.
        if (self._job_client.config.embedding_model_provider == "openai") and (
            source_columns := get_columns_names_with_prop(self._load_table, VECTORIZE_HINT)
        ):
            arrow_table = fill_empty_source_column_values_with_placeholder(
                arrow_table, source_columns, EMPTY_STRING_PLACEHOLDER
            )

        # get_first_column_name_with_prop(self._load_table, "row_key"),
        write_records(
            arrow_table,
            db_client=db_client,
            table_name=fq_table_name,
            write_disposition=write_disposition,
            # use deterministic
            merge_key=SqlMergeFollowupJob.get_row_key_col(
                [self._load_table],
                self._load_table,
                self._job_client.dataset_name,
                self._job_client.dataset_name,
            ),
        )


class LanceDBRemoveOrphansJob(RunnableLoadJob):
    def __init__(
        self,
        file_path: str,
    ) -> None:
        from dlt.destinations.impl.lancedb.lancedb_client import LanceDBClient

        super().__init__(file_path)
        self._job_client: "LanceDBClient" = None
        self.references = ReferenceFollowupJobRequest.resolve_references(file_path)

    def run(self) -> None:
        db_client: DBConnection = self._job_client.db_client
        table_lineage: TTableLineage = [
            TableJob(
                # TODO: prepare table
                table_schema=self._schema.get_table(
                    ParsedLoadJobFileName.parse(file_path_).table_name
                ),
                table_name=ParsedLoadJobFileName.parse(file_path_).table_name,
                file_path=file_path_,
            )
            for file_path_ in self.references
        ]

        for job in table_lineage:
            target_is_root_table = not is_nested_table(job.table_schema)
            fq_table_name = self._job_client.make_qualified_table_name(job.table_name)
            file_path = job.file_path
            with open(file_path, mode="rb") as f:
                payload_arrow_table: pa.Table = pq.read_table(f)

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

            write_records(
                payload_arrow_table,
                db_client=db_client,
                table_name=fq_table_name,
                write_disposition="merge",
                merge_key=merge_key,
                remove_orphans=True,
                delete_condition=delete_condition,
            )
