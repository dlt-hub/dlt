import os
from pathlib import Path
from typing import ClassVar, Dict, Optional, Sequence, Tuple, List, cast, Type, Any
import google.cloud.bigquery as bigquery  # noqa: I250
from google.cloud import exceptions as gcp_exceptions
from google.api_core import exceptions as api_core_exceptions

from dlt.common import json, logger
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import (
    FollowupJob,
    NewLoadJob,
    TLoadJobState,
    LoadJob,
    SupportsStagingDestination,
)
from dlt.common.data_types import TDataType
from dlt.common.storages.file_storage import FileStorage
from dlt.common.schema import TColumnSchema, Schema, TTableSchemaColumns
from dlt.common.schema.typing import TTableSchema, TColumnType, TTableFormat
from dlt.common.schema.exceptions import UnknownTableException

from dlt.destinations.job_client_impl import SqlJobClientWithStaging
from dlt.destinations.exceptions import (
    DestinationSchemaWillNotUpdate,
    DestinationTransientException,
    LoadJobNotExistsException,
    LoadJobTerminalException,
)

from dlt.destinations.impl.bigquery import capabilities
from dlt.destinations.impl.bigquery.configuration import BigQueryClientConfiguration
from dlt.destinations.impl.bigquery.sql_client import BigQuerySqlClient, BQ_TERMINAL_REASONS
from dlt.destinations.sql_jobs import SqlMergeJob, SqlStagingCopyJob, SqlJobParams
from dlt.destinations.job_impl import NewReferenceJob
from dlt.destinations.sql_client import SqlClientBase
from dlt.destinations.type_mapping import TypeMapper

from dlt.common.schema.utils import table_schema_has_type


class BigQueryTypeMapper(TypeMapper):
    sct_to_unbound_dbt = {
        "complex": "JSON",
        "text": "STRING",
        "double": "FLOAT64",
        "bool": "BOOLEAN",
        "date": "DATE",
        "timestamp": "TIMESTAMP",
        "bigint": "INTEGER",
        "binary": "BYTES",
        "wei": "BIGNUMERIC",  # non parametrized should hold wei values
        "time": "TIME",
    }

    sct_to_dbt = {
        "text": "STRING(%i)",
        "binary": "BYTES(%i)",
        "decimal": "NUMERIC(%i,%i)",
    }

    dbt_to_sct = {
        "STRING": "text",
        "FLOAT": "double",
        "BOOLEAN": "bool",
        "DATE": "date",
        "TIMESTAMP": "timestamp",
        "INTEGER": "bigint",
        "BYTES": "binary",
        "NUMERIC": "decimal",
        "BIGNUMERIC": "decimal",
        "JSON": "complex",
        "TIME": "time",
    }

    def from_db_type(
        self, db_type: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        if db_type == "BIGNUMERIC":
            if precision is None:  # biggest numeric possible
                return dict(data_type="wei")
        return super().from_db_type(db_type, precision, scale)


class BigQueryLoadJob(LoadJob, FollowupJob):
    def __init__(
        self,
        file_name: str,
        bq_load_job: bigquery.LoadJob,
        http_timeout: float,
        retry_deadline: float,
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
                    logger.warning(
                        f"Got reason {reason} for job {self.file_name}, job considered still"
                        f" running. ({self.bq_load_job.error_result})"
                    )
                    # status of the job could not be obtained, job still running
                    return "running"
                else:
                    # retry on all other reasons, including `backendError` which requires retry when the job is done
                    return "retry"
        else:
            return "running"

    def bigquery_job_id(self) -> str:
        return BigQueryLoadJob.get_job_id_from_file_path(super().file_name())

    def exception(self) -> str:
        exception: str = json.dumps(
            {
                "error_result": self.bq_load_job.error_result,
                "errors": self.bq_load_job.errors,
                "job_start": self.bq_load_job.started,
                "job_end": self.bq_load_job.ended,
                "job_id": self.bq_load_job.job_id,
            }
        )
        return exception

    @staticmethod
    def get_job_id_from_file_path(file_path: str) -> str:
        return Path(file_path).name.replace(".", "_")


class BigQueryMergeJob(SqlMergeJob):
    @classmethod
    def gen_key_table_clauses(
        cls,
        root_table_name: str,
        staging_root_table_name: str,
        key_clauses: Sequence[str],
        for_delete: bool,
    ) -> List[str]:
        # generate several clauses: BigQuery does not support OR nor unions
        sql: List[str] = []
        for clause in key_clauses:
            sql.append(
                f"FROM {root_table_name} AS d WHERE EXISTS (SELECT 1 FROM"
                f" {staging_root_table_name} AS s WHERE {clause.format(d='d', s='s')})"
            )
        return sql


class BigqueryStagingCopyJob(SqlStagingCopyJob):
    @classmethod
    def generate_sql(
        cls,
        table_chain: Sequence[TTableSchema],
        sql_client: SqlClientBase[Any],
        params: Optional[SqlJobParams] = None,
    ) -> List[str]:
        sql: List[str] = []
        for table in table_chain:
            with sql_client.with_staging_dataset(staging=True):
                staging_table_name = sql_client.make_qualified_table_name(table["name"])
            table_name = sql_client.make_qualified_table_name(table["name"])
            # drop destination table
            sql.append(f"DROP TABLE IF EXISTS {table_name};")
            # recreate destination table with data cloned from staging table
            sql.append(f"CREATE TABLE {table_name} CLONE {staging_table_name};")
        return sql


class BigQueryClient(SqlJobClientWithStaging, SupportsStagingDestination):
    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, schema: Schema, config: BigQueryClientConfiguration) -> None:
        sql_client = BigQuerySqlClient(
            config.normalize_dataset_name(schema),
            config.credentials,
            config.get_location(),
            config.http_timeout,
            config.retry_deadline,
        )
        super().__init__(schema, config, sql_client)
        self.config: BigQueryClientConfiguration = config
        self.sql_client: BigQuerySqlClient = sql_client  # type: ignore
        self.type_mapper = BigQueryTypeMapper(self.capabilities)

    def _create_merge_followup_jobs(self, table_chain: Sequence[TTableSchema]) -> List[NewLoadJob]:
        return [BigQueryMergeJob.from_table_chain(table_chain, self.sql_client)]

    def _create_replace_followup_jobs(
        self, table_chain: Sequence[TTableSchema]
    ) -> List[NewLoadJob]:
        if self.config.replace_strategy == "staging-optimized":
            return [BigqueryStagingCopyJob.from_table_chain(table_chain, self.sql_client)]
        return super()._create_replace_followup_jobs(table_chain)

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
                    self.config.retry_deadline,
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
                    self.config.retry_deadline,
                )
            except api_core_exceptions.GoogleAPICallError as gace:
                reason = BigQuerySqlClient._get_reason_from_errors(gace)
                if reason == "notFound":
                    # google.api_core.exceptions.NotFound: 404 - table not found
                    raise UnknownTableException(table["name"])
                elif reason == "duplicate":
                    # google.api_core.exceptions.Conflict: 409 PUT - already exists
                    return self.restore_file_load(file_path)
                elif reason in BQ_TERMINAL_REASONS:
                    # google.api_core.exceptions.BadRequest - will not be processed ie bad job name
                    raise LoadJobTerminalException(file_path, f"The server reason was: {reason}")
                else:
                    raise DestinationTransientException(gace)
        return job

    def _get_table_update_sql(
        self,
        table_name: str,
        new_columns: Sequence[TColumnSchema],
        generate_alter: bool,
        separate_alters: bool = False,
    ) -> List[str]:
        sql = super()._get_table_update_sql(table_name, new_columns, generate_alter)
        canonical_name = self.sql_client.make_qualified_table_name(table_name)

        cluster_list = [
            self.capabilities.escape_identifier(c["name"]) for c in new_columns if c.get("cluster")
        ]
        partition_list = [
            self.capabilities.escape_identifier(c["name"])
            for c in new_columns
            if c.get("partition")
        ]

        # partition by must be added first
        if len(partition_list) > 0:
            if len(partition_list) > 1:
                raise DestinationSchemaWillNotUpdate(
                    canonical_name, partition_list, "Partition requested for more than one column"
                )
            else:
                sql[0] = sql[0] + f"\nPARTITION BY DATE({partition_list[0]})"
        if len(cluster_list) > 0:
            sql[0] = sql[0] + "\nCLUSTER BY " + ",".join(cluster_list)

        return sql

    def _get_column_def_sql(self, c: TColumnSchema, table_format: TTableFormat = None) -> str:
        name = self.capabilities.escape_identifier(c["name"])
        return (
            f"{name} {self.type_mapper.to_db_type(c, table_format)} {self._gen_not_null(c.get('nullable', True))}"
        )

    def get_storage_table(self, table_name: str) -> Tuple[bool, TTableSchemaColumns]:
        schema_table: TTableSchemaColumns = {}
        try:
            table = self.sql_client.native_connection.get_table(
                self.sql_client.make_qualified_table_name(table_name, escape=False),
                retry=self.sql_client._default_retry,
                timeout=self.config.http_timeout,
            )
            partition_field = table.time_partitioning.field if table.time_partitioning else None
            for c in table.schema:
                schema_c: TColumnSchema = {
                    "name": c.name,
                    "nullable": c.is_nullable,
                    "unique": False,
                    "sort": False,
                    "primary_key": False,
                    "foreign_key": False,
                    "cluster": c.name in (table.clustering_fields or []),
                    "partition": c.name == partition_field,
                    **self._from_db_type(c.field_type, c.precision, c.scale),
                }
                schema_table[c.name] = schema_c
            return True, schema_table
        except gcp_exceptions.NotFound:
            return False, schema_table

    def _create_load_job(self, table: TTableSchema, file_path: str) -> bigquery.LoadJob:
        # append to table for merge loads (append to stage) and regular appends
        table_name = table["name"]

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
                raise LoadJobTerminalException(
                    file_path,
                    "Bigquery cannot load into JSON data type from parquet. Use jsonl instead.",
                )
            source_format = bigquery.SourceFormat.PARQUET
            # parquet needs NUMERIC type autodetection
            decimal_target_types = ["NUMERIC", "BIGNUMERIC"]

        job_id = BigQueryLoadJob.get_job_id_from_file_path(file_path)
        job_config = bigquery.LoadJobConfig(
            autodetect=False,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
            source_format=source_format,
            decimal_target_types=decimal_target_types,
            ignore_unknown_values=False,
            max_bad_records=0,
        )

        if bucket_path:
            return self.sql_client.native_connection.load_table_from_uri(
                bucket_path,
                self.sql_client.make_qualified_table_name(table_name, escape=False),
                job_id=job_id,
                job_config=job_config,
                timeout=self.config.file_upload_timeout,
            )

        with open(file_path, "rb") as f:
            return self.sql_client.native_connection.load_table_from_file(
                f,
                self.sql_client.make_qualified_table_name(table_name, escape=False),
                job_id=job_id,
                job_config=job_config,
                timeout=self.config.file_upload_timeout,
            )

    def _retrieve_load_job(self, file_path: str) -> bigquery.LoadJob:
        job_id = BigQueryLoadJob.get_job_id_from_file_path(file_path)
        return cast(bigquery.LoadJob, self.sql_client.native_connection.get_job(job_id))

    def _from_db_type(
        self, bq_t: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        return self.type_mapper.from_db_type(bq_t, precision, scale)
