import os
from pathlib import Path
from typing import ClassVar, Dict, Optional, Sequence, Tuple, List, cast, Type
import google.cloud.bigquery as bigquery  # noqa: I250
from google.cloud import exceptions as gcp_exceptions
from google.api_core import exceptions as api_core_exceptions

from dlt.common import json, logger
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import FollowupJob, NewLoadJob, TLoadJobState, LoadJob
from dlt.common.data_types import TDataType
from dlt.common.storages.file_storage import FileStorage
from dlt.common.schema import TColumnSchema, Schema, TTableSchemaColumns
from dlt.common.schema.typing import TTableSchema, TWriteDisposition

from dlt.destinations.job_client_impl import SqlJobClientBase
from dlt.destinations.exceptions import DestinationSchemaWillNotUpdate, DestinationTransientException, LoadJobNotExistsException, LoadJobTerminalException, LoadJobUnknownTableException

from dlt.destinations.bigquery import capabilities
from dlt.destinations.bigquery.configuration import BigQueryClientConfiguration
from dlt.destinations.bigquery.sql_client import BigQuerySqlClient, BQ_TERMINAL_REASONS
from dlt.destinations.sql_merge_job import SqlMergeJob
from dlt.destinations.job_impl import NewReferenceJob

from dlt.common.schema.utils import table_schema_has_type

SCT_TO_BQT: Dict[TDataType, str] = {
    "complex": "JSON",
    "text": "STRING",
    "double": "FLOAT64",
    "bool": "BOOLEAN",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "bigint": "INTEGER",
    "binary": "BYTES",
    "decimal": "NUMERIC(%i,%i)",
    "wei": "BIGNUMERIC"  # non parametrized should hold wei values
}

BQT_TO_SCT: Dict[str, TDataType] = {
    "STRING": "text",
    "FLOAT": "double",
    "BOOLEAN": "bool",
    "DATE": "date",
    "TIMESTAMP": "timestamp",
    "INTEGER": "bigint",
    "BYTES": "binary",
    "NUMERIC": "decimal",
    "BIGNUMERIC": "decimal",
    "JSON": "complex"
}

class BigQueryLoadJob(LoadJob, FollowupJob):
    def __init__(
        self,
        file_name: str,
        bq_load_job: bigquery.LoadJob,
        http_timeout: float,
        retry_deadline: float
    ) -> None:
        self.bq_load_job = bq_load_job
        self.default_retry = bigquery.DEFAULT_RETRY.with_deadline(retry_deadline)
        self.http_timeout = http_timeout
        super().__init__(file_name)

    def state(self) -> TLoadJobState:
        # check server if done
        done = self.bq_load_job.done(retry=self.default_retry, timeout=self.http_timeout)
        if done:
            # rows processed
            if self.bq_load_job.output_rows is not None and self.bq_load_job.error_result is None:
                return "completed"
            else:
                reason = self.bq_load_job.error_result.get("reason")
                if reason in BQ_TERMINAL_REASONS:
                    # the job permanently failed for the reason above
                    return "failed"
                elif reason in ["internalError"]:
                    logger.warning(f"Got reason {reason} for job {self.file_name}, job considered still running. ({self.bq_load_job.error_result})")
                    # status of the job could not be obtained, job still running
                    return "running"
                else:
                    # retry on all other reasons, including `backendError` which requires retry when the job is done
                    return "retry"
        else:
            return "running"

    def job_id(self) -> str:
        return BigQueryLoadJob.get_job_id_from_file_path(super().job_id())

    def exception(self) -> str:
        exception: str = json.dumps({
            "error_result": self.bq_load_job.error_result,
            "errors": self.bq_load_job.errors,
            "job_start": self.bq_load_job.started,
            "job_end": self.bq_load_job.ended,
            "job_id": self.bq_load_job.job_id
        })
        return exception

    @staticmethod
    def get_job_id_from_file_path(file_path: str) -> str:
        return Path(file_path).name.replace(".", "_")


class BigQueryMergeJob(SqlMergeJob):

    @classmethod
    def gen_key_table_clauses(cls, root_table_name: str, staging_root_table_name: str, key_clauses: Sequence[str], for_delete: bool) -> List[str]:
        # generate several clauses: BigQuery does not support OR nor unions
        sql: List[str] = []
        for clause in key_clauses:
            sql.append(f"FROM {root_table_name} AS d WHERE EXISTS (SELECT 1 FROM {staging_root_table_name} AS s WHERE {clause.format(d='d', s='s')})")
        return sql


class BigQueryClient(SqlJobClientBase):

    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, schema: Schema, config: BigQueryClientConfiguration) -> None:
        sql_client = BigQuerySqlClient(
            self.make_dataset_name(schema, config.dataset_name, config.default_schema_name),
            config.credentials,
            config.get_location(),
            config.http_timeout,
            config.retry_deadline
        )
        super().__init__(schema, config, sql_client)
        self.config: BigQueryClientConfiguration = config
        self.sql_client: BigQuerySqlClient = sql_client  # type: ignore

    def create_merge_job(self, table_chain: Sequence[TTableSchema]) -> NewLoadJob:
        return BigQueryMergeJob.from_table_chain(table_chain, self.sql_client)

    def restore_file_load(self, file_path: str) -> LoadJob:
        """Returns a completed SqlLoadJob or restored BigQueryLoadJob

        See base class for details on SqlLoadJob. BigQueryLoadJob is restored with job id derived from `file_path`

        Args:
            file_path (str): a path to a job file

        Returns:
            LoadJob: completed SqlLoadJob or restored BigQueryLoadJob
        """
        job = super().restore_file_load(file_path)
        if not job:
            try:
                job = BigQueryLoadJob(
                    FileStorage.get_file_name_from_file_path(file_path),
                    self._retrieve_load_job(file_path),
                    self.config.http_timeout,
                    self.config.retry_deadline
                )
            except api_core_exceptions.GoogleAPICallError as gace:
                reason = BigQuerySqlClient._get_reason_from_errors(gace)
                if reason == "notFound":
                    raise LoadJobNotExistsException(file_path)
                elif reason in BQ_TERMINAL_REASONS:
                    raise LoadJobTerminalException(file_path, f"The server reason was: {reason}")
                else:
                    raise DestinationTransientException(gace)
        return job

    def start_file_load(self, table: TTableSchema, file_path: str, load_id: str) -> LoadJob:
        job = super().start_file_load(table, file_path, load_id)

        if not job:
            try:
                job = BigQueryLoadJob(
                    FileStorage.get_file_name_from_file_path(file_path),
                    self._create_load_job(table, file_path),
                    self.config.http_timeout,
                    self.config.retry_deadline
                )
            except api_core_exceptions.GoogleAPICallError as gace:
                reason = BigQuerySqlClient._get_reason_from_errors(gace)
                if reason == "notFound":
                    # google.api_core.exceptions.NotFound: 404 - table not found
                    raise LoadJobUnknownTableException(table["name"], file_path)
                elif reason == "duplicate":
                    # google.api_core.exceptions.Conflict: 409 PUT - already exists
                    return self.restore_file_load(file_path)
                elif reason in BQ_TERMINAL_REASONS:
                    # google.api_core.exceptions.BadRequest - will not be processed ie bad job name
                    raise LoadJobTerminalException(file_path, f"The server reason was: {reason}")
                else:
                    raise DestinationTransientException(gace)
        return job

    def _get_table_update_sql(self, table_name: str, new_columns: Sequence[TColumnSchema], generate_alter: bool, separate_alters: bool = False) -> List[str]:
        sql = super()._get_table_update_sql(table_name, new_columns, generate_alter)
        canonical_name = self.sql_client.make_qualified_table_name(table_name)

        cluster_list = [self.capabilities.escape_identifier(c["name"]) for c in new_columns if c.get("cluster")]
        partition_list = [self.capabilities.escape_identifier(c["name"]) for c in new_columns if c.get("partition")]

        # partition by must be added first
        if len(partition_list) > 0:
            if len(partition_list) > 1:
                raise DestinationSchemaWillNotUpdate(canonical_name, partition_list, "Partition requested for more than one column")
            else:
                sql[0] = sql[0] + f"\nPARTITION BY DATE({partition_list[0]})"
        if len(cluster_list) > 0:
            sql[0] = sql[0] + "\nCLUSTER BY " + ",".join(cluster_list)

        return sql

    def _get_column_def_sql(self, c: TColumnSchema) -> str:
        name = self.capabilities.escape_identifier(c["name"])
        return f"{name} {self._to_db_type(c['data_type'])} {self._gen_not_null(c['nullable'])}"

    def get_storage_table(self, table_name: str) -> Tuple[bool, TTableSchemaColumns]:
        schema_table: TTableSchemaColumns = {}
        try:
            table = self.sql_client.native_connection.get_table(
                self.sql_client.make_qualified_table_name(table_name, escape=False),
                retry=self.sql_client._default_retry,
                timeout=self.config.http_timeout
            )
            partition_field = table.time_partitioning.field if table.time_partitioning else None
            for c in table.schema:
                schema_c: TColumnSchema = {
                    "name": c.name,
                    "nullable": c.is_nullable,
                    "data_type": self._from_db_type(c.field_type, c.precision, c.scale),
                    "unique": False,
                    "sort": False,
                    "primary_key": False,
                    "foreign_key": False,
                    "cluster": c.name in (table.clustering_fields or []),
                    "partition": c.name == partition_field
                }
                schema_table[c.name] = schema_c
            return True, schema_table
        except gcp_exceptions.NotFound:
            return False, schema_table

    def _create_load_job(self, table: TTableSchema, file_path: str) -> bigquery.LoadJob:
        table_name = table["name"]
        write_disposition = table["write_disposition"]
        # append to table for merge loads (append to stage) and regular appends
        bq_wd = bigquery.WriteDisposition.WRITE_TRUNCATE if write_disposition == "replace" else bigquery.WriteDisposition.WRITE_APPEND

        # determine wether we load from local or uri
        bucket_path = None
        ext: str = os.path.splitext(file_path)[1][1:]
        if NewReferenceJob.is_reference_job(file_path):
            bucket_path = NewReferenceJob.resolve_reference(file_path)
            ext = os.path.splitext(bucket_path)[1][1:]

        # choose correct source format
        source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        decimal_target_types: List[str] = None
        if ext == "parquet":
            # if table contains complex types, we cannot load with parquet
            if table_schema_has_type(table, "complex"):
                raise LoadJobTerminalException(file_path, "Bigquery cannot load into JSON data type from parquet. Use jsonl instead.")
            source_format = bigquery.SourceFormat.PARQUET
            # parquet needs NUMERIC type autodetection
            decimal_target_types = ["NUMERIC", "BIGNUMERIC"]

        # if merge then load to staging
        with self.sql_client.with_staging_dataset(write_disposition == "merge"):
            job_id = BigQueryLoadJob.get_job_id_from_file_path(file_path)
            job_config = bigquery.LoadJobConfig(
                autodetect=False,
                write_disposition=bq_wd,
                create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
                source_format=source_format,
                decimal_target_types=decimal_target_types,
                ignore_unknown_values=False,
                max_bad_records=0)

            if bucket_path:
                return self.sql_client.native_connection.load_table_from_uri(
                        bucket_path,
                        self.sql_client.make_qualified_table_name(table_name, escape=False),
                        job_id=job_id,
                        job_config=job_config,
                        timeout=self.config.file_upload_timeout
                    )

            with open(file_path, "rb") as f:
                return self.sql_client.native_connection.load_table_from_file(
                        f,
                        self.sql_client.make_qualified_table_name(table_name, escape=False),
                        job_id=job_id,
                        job_config=job_config,
                        timeout=self.config.file_upload_timeout
                    )

    def _retrieve_load_job(self, file_path: str) -> bigquery.LoadJob:
        job_id = BigQueryLoadJob.get_job_id_from_file_path(file_path)
        return cast(bigquery.LoadJob, self.sql_client.native_connection.get_job(job_id))

    @classmethod
    def _to_db_type(cls, sc_t: TDataType) -> str:
        if sc_t == "decimal":
            return SCT_TO_BQT["decimal"] % cls.capabilities.decimal_precision
        return SCT_TO_BQT[sc_t]

    @classmethod
    def _from_db_type(cls, bq_t: str, precision: Optional[int], scale: Optional[int]) -> TDataType:
        if bq_t == "BIGNUMERIC":
            if precision is None:  # biggest numeric possible
                return "wei"
        return BQT_TO_SCT.get(bq_t, "text")


