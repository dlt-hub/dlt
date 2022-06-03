
from pathlib import Path
from typing import Any, AnyStr, Dict, List, Literal, Optional, Tuple, Type
import google.cloud.bigquery as bigquery
from google.cloud import exceptions as gcp_exceptions
from google.oauth2 import service_account
from google.api_core import exceptions as api_core_exceptions


from dlt.common import json, logger
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.configuration import GcpClientConfiguration
from dlt.common.dataset_writers import TWriterType, escape_bigquery_identifier
from dlt.loaders.local_types import LoadJobStatus
from dlt.common.schema import Column, DataType, Schema, Table

from dlt.loaders.client_base import SqlClientBase, LoadJob
from dlt.loaders.exceptions import LoadClientSchemaWillNotUpdate, LoadJobNotExistsException, LoadJobServerTerminalException, LoadUnknownTableException

SCT_TO_BQT: Dict[DataType, str] = {
    "text": "STRING",
    "double": "FLOAT64",
    "bool": "BOOLEAN",
    "timestamp": "TIMESTAMP",
    "bigint": "INTEGER",
    "binary": "BYTES",
    "decimal": f"NUMERIC({DEFAULT_NUMERIC_PRECISION},{DEFAULT_NUMERIC_SCALE})",
    "wei": "BIGNUMERIC"  # non parametrized should hold wei values
}

BQT_TO_SCT: Dict[str, DataType] = {
    "STRING": "text",
    "FLOAT": "double",
    "BOOLEAN": "bool",
    "TIMESTAMP": "timestamp",
    "INTEGER": "bigint",
    "BYTES": "binary",
    "NUMERIC": "decimal",
    "BIGNUMERIC": "decimal"
}

class BigQueryLoadJob(LoadJob):
    def __init__(self, file_name: str, bq_load_job: bigquery.LoadJob, CONFIG: Type[GcpClientConfiguration]) -> None:
        self.bq_load_job = bq_load_job
        self.C = CONFIG
        self.default_retry = bigquery.DEFAULT_RETRY.with_deadline(CONFIG.TIMEOUT)
        super().__init__(file_name)

    def status(self) -> LoadJobStatus:
        # check server if done
        done = self.bq_load_job.done(retry=self.default_retry, timeout=self.C.TIMEOUT)
        if done:
            # rows processed
            if self.bq_load_job.output_rows is not None and self.bq_load_job.error_result is None:
                return "completed"
            else:
                return "failed"
        else:
            return "running"

    def file_name(self) -> str:
        return self._file_name

    def exception(self) -> str:
        exception: str = json.dumps({
            "error_result": self.bq_load_job.error_result,
            "errors": self.bq_load_job.errors,
            "job_start": self.bq_load_job.started,
            "job_end": self.bq_load_job.ended,
            "job_id": self.bq_load_job.job_id
        })
        return exception


class BigQueryClient(SqlClientBase):
    def __init__(self, schema: Schema, CONFIG: Type[GcpClientConfiguration]) -> None:
        self._client: bigquery.Client = None
        self.C = CONFIG
        self.default_retry = bigquery.DEFAULT_RETRY.with_deadline(CONFIG.TIMEOUT)
        super().__init__(schema)


    def initialize_storage(self) -> None:
        dataset_name = self._to_canonical_schema_name()
        try:
            self._client.get_dataset(dataset_name, retry=self.default_retry, timeout=self.C.TIMEOUT)
        except gcp_exceptions.NotFound:
            self._client.create_dataset(dataset_name, exists_ok=False, retry=self.default_retry, timeout=self.C.TIMEOUT)

    def get_file_load(self, file_path: str) -> LoadJob:
        try:
            return BigQueryLoadJob(
                SqlClientBase.get_file_name_from_file_path(file_path),
                self._retrieve_load_job(file_path),
                self.C
            )
        except api_core_exceptions.NotFound:
            raise LoadJobNotExistsException(file_path)
        except (api_core_exceptions.BadRequest, api_core_exceptions.NotFound):
            raise LoadJobServerTerminalException(file_path)

    def start_file_load(self, table_name: str, file_path: str) -> LoadJob:
        # verify that table exists in the schema
        self._get_table_by_name(table_name, file_path)
        try:
            return BigQueryLoadJob(
                SqlClientBase.get_file_name_from_file_path(file_path),
                self._create_load_job(table_name, file_path),
                self.C
            )
        except api_core_exceptions.NotFound:
            # google.api_core.exceptions.BadRequest - will not be processed ie bad job name
            raise LoadUnknownTableException(table_name, file_path)
        except (api_core_exceptions.BadRequest, api_core_exceptions.NotFound):
            # google.api_core.exceptions.NotFound: 404 - table not found
            raise LoadJobServerTerminalException(file_path)
        except api_core_exceptions.Conflict:
            # google.api_core.exceptions.Conflict: 409 PUT - already exists
            return self.get_file_load(file_path)

    def update_storage_schema(self) -> None:
        storage_version = self._get_schema_version_from_storage()
        if storage_version < self.schema.schema_version:
            for sql in self._build_schema_update_sql():
                self._execute_sql(sql)
            self._update_schema_version(self.schema.schema_version)

    def _open_connection(self) -> None:
        credentials = service_account.Credentials.from_service_account_info(self.C.to_service_credentials())
        self._client = bigquery.Client(self.C.PROJECT_ID, credentials=credentials)

    def _close_connection(self) -> None:
        if self._client:
            self._client.close()
            self._client = None

    def _get_schema_version_from_storage(self) -> int:
        try:
            return super()._get_schema_version_from_storage()
        except api_core_exceptions.NotFound:
            # there's no table so there's no schema
            return 0

    def _build_schema_update_sql(self) -> List[str]:
        sql_updates = []
        for table_name in self.schema.schema_tables:
            exists, storage_table = self._get_storage_table(table_name)
            sql = self._get_table_update_sql(table_name, storage_table, exists)
            if sql:
                sql_updates.append(sql)
        return sql_updates

    def _get_table_update_sql(self, table_name: str, storage_table: Table, exists: bool) -> str:
        new_columns = self._create_table_update(table_name, storage_table)
        if len(new_columns) == 0:
            # no changes
            return None
        # build sql
        canonical_name = self._to_canonical_table_name(table_name)
        if not exists:
            # build CREATE
            sql = f"CREATE TABLE {canonical_name} (\n"
            sql += ",\n".join([self._get_column_def_sql(c) for c in new_columns])
            sql += ")"
        else:
            # build ALTER
            sql = f"ALTER TABLE {canonical_name}\n"
            sql += ",\n".join(["ADD COLUMN " + self._get_column_def_sql(c) for c in new_columns])
        # scan columns to get hints
        cluster_list = [escape_bigquery_identifier(c["name"]) for c in new_columns if c.get("cluster", False)]
        partition_list = [escape_bigquery_identifier(c["name"]) for c in new_columns if c.get("partition", False)]
        # partition by must be added first
        if len(partition_list) > 0:
            if exists:
                raise LoadClientSchemaWillNotUpdate(canonical_name, partition_list, "Partition requested after table was created")
            elif len(partition_list) > 1:
                raise LoadClientSchemaWillNotUpdate(canonical_name, partition_list, "Partition requested for more than one column")
            else:
                sql += f"\nPARTITION BY DATE({partition_list[0]})"
        if len(cluster_list) > 0:
            if exists:
                raise LoadClientSchemaWillNotUpdate(canonical_name, cluster_list, "Clustering requested after table was created")
            else:
                sql += "\nCLUSTER BY " + ",".join(cluster_list)

        return sql

    def _get_column_def_sql(self, c: Column) -> str:
        name = escape_bigquery_identifier(c["name"])
        return f"{name} {self._sc_t_to_bq_t(c['data_type'])} {self._gen_not_null(c['nullable'])}"

    def _get_storage_table(self, table_name: str) -> Tuple[bool, Table]:
        schema_table: Table = {}
        try:
            table = self._client.get_table(self._to_canonical_table_name(table_name), retry=self.default_retry, timeout=self.C.TIMEOUT)
            partition_field = table.time_partitioning.field if table.time_partitioning else None
            for c in table.schema:
                schema_c: Column = {
                    "name": c.name,
                    "nullable": c.is_nullable,
                    "data_type": self._bq_t_to_sc_t(c.field_type, c.precision, c.scale),
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

    def _execute_sql(self, query: AnyStr) -> Any:
        logger.debug(f"Will execute query {query}")  # type: ignore
        return self._client.query(query, job_retry=self.default_retry, timeout=self.C.TIMEOUT).result()

    def _to_canonical_schema_name(self) -> str:
        return f"{self.C.PROJECT_ID}.{self.C.DATASET}_{self.schema.schema_name}"

    def _create_load_job(self, table_name: str, file_path: str) -> bigquery.LoadJob:
        job_id = BigQueryClient._get_job_id_from_file_path(file_path)
        job_config = bigquery.LoadJobConfig(
            autodetect=False,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            ignore_unknown_values=False,
            max_bad_records=0,

            )
        with open(file_path, "rb") as f:
            return self._client.load_table_from_file(f,
                                                     self._to_canonical_table_name(table_name),
                                                     job_id=job_id,
                                                     job_config=job_config,
                                                     timeout=self.C.TIMEOUT
                                                    )

    def _retrieve_load_job(self, file_path: str) -> bigquery.LoadJob:
        job_id = BigQueryClient._get_job_id_from_file_path(file_path)
        return self._client.get_job(job_id)

    @staticmethod
    def _get_job_id_from_file_path(file_path: str) -> str:
        return Path(file_path).name.replace(".", "_")

    @staticmethod
    def _gen_not_null(v: bool) -> str:
        return "NOT NULL" if not v else ""

    @staticmethod
    def _sc_t_to_bq_t(sc_t: DataType) -> str:
        return SCT_TO_BQT[sc_t]

    @staticmethod
    def _bq_t_to_sc_t(bq_t: str, precision: Optional[int], scale: Optional[int]) -> DataType:
        if bq_t == "BIGNUMERIC":
            if precision is None:  # biggest numeric possible
                return "wei"
        return BQT_TO_SCT.get(bq_t, "text")


def make_client(schema: Schema, C: Type[GcpClientConfiguration]) -> BigQueryClient:
    return BigQueryClient(schema, C)


def supported_writer(C: Type[GcpClientConfiguration]) -> TWriterType:
    return "jsonl"

# cred = service_account.Credentials.from_service_account_info(_credentials)
# project_id = cred.get('project_id')
# client = bigquery.Client(project_id, credentials=cred)
# print(client.get_dataset("carbon_bot_extract_7"))
# exit(0)
# from dlt.common.configuration import SchemaStoreConfiguration
# from dlt.common.logger import  init_logging_from_config

# init_logging_from_config(CLIENT_CONFIG)

# schema = Schema(SchemaStoreConfiguration.TRACKER_SCHEMA_FILE_PATH)
# schema.load_schema()
# import pprint
# # pprint.pprint(schema.as_yaml())
# with make_client(schema) as client:
#     client.initialize_storage()
#     # job = client._create_load_job("tracker", "_storage/loaded/1630949263.574516/completed_jobs/tracker.1c31ff1b-c250-4690-8973-14f0ee9ae355.jsonl")
#     # unk table
#     # job = client._create_load_job("trackerZ", "_storage/loaded/1630949263.574516/completed_jobs/tracker.4876f905-aefe-4262-a440-d29ed2643c3a.jsonl")
#     # job = client._create_load_job("tracker", "_storage/loaded/1630949263.574516/completed_jobs/event_bot.c9105079-2d1d-4ad3-8613-a5dff790889d.jsonl")
#     # failed
#     # job = client._retrieve_load_job("_storage/loaded/1630949263.574516/completed_jobs/event_bot.c9105079-2d1d-4ad3-8613-a5dff790889d.jsonl")
#     # OK
#     job = client._retrieve_load_job("_storage/loaded/1630949263.574516/completed_jobs/tracker.1c31ff1b-c250-4690-8973-14f0ee9ae355.jsonl")
#     while True:
#         try:
#             # this does not throw
#             done = job.done()
#             print(f"DONE: {job.done(reload=False)}")
#         except Exception as e:
#             logger.exception("DONE")
#             done = True
#         if done:
#             break;
#         # done is not self running

#         # print(job.running())
#         sleep(1)
#     try:
#         print(f"status: {job.state}")
#         print(f"error: {job.error_result}")
#         print(f"errors: {job.errors}")
#         print(f"line count: {job.output_rows}")
#         print(job.exception())
#     except:
#         logger.exception("EXCEPTION")
#     try:
#         print(job.result())
#     except:
#         logger.exception("RESULT")

    # non existing table
    # wrong data - unknown column

