import functools
import os
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Optional, Sequence, Tuple, Type, cast

import google.cloud.bigquery as bigquery  # noqa: I250
from google.api_core import exceptions as api_core_exceptions
from google.cloud import exceptions as gcp_exceptions
from google.api_core import retry
from google.cloud.bigquery.retry import _RETRYABLE_REASONS

from dlt.common import logger
from dlt.common.json import json
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import (
    FollowupJob,
    NewLoadJob,
    TLoadJobState,
    LoadJob,
    SupportsStagingDestination,
)
from dlt.common.schema import TColumnSchema, Schema, TTableSchemaColumns
from dlt.common.schema.exceptions import UnknownTableException
from dlt.common.schema.typing import TTableSchema, TColumnType, TTableFormat
from dlt.common.schema.utils import get_inherited_table_hint
from dlt.common.schema.utils import table_schema_has_type
from dlt.common.storages.file_storage import FileStorage
from dlt.common.typing import DictStrAny
from dlt.destinations.job_impl import DestinationJsonlLoadJob, DestinationParquetLoadJob
from dlt.destinations.sql_client import SqlClientBase
from dlt.destinations.exceptions import (
    DestinationSchemaWillNotUpdate,
    DestinationTerminalException,
    DestinationTransientException,
    LoadJobNotExistsException,
    LoadJobTerminalException,
)
from dlt.destinations.impl.bigquery import capabilities
from dlt.destinations.impl.bigquery.bigquery_adapter import (
    PARTITION_HINT,
    CLUSTER_HINT,
    TABLE_DESCRIPTION_HINT,
    ROUND_HALF_EVEN_HINT,
    ROUND_HALF_AWAY_FROM_ZERO_HINT,
    TABLE_EXPIRATION_HINT,
)
from dlt.destinations.impl.bigquery.configuration import BigQueryClientConfiguration
from dlt.destinations.impl.bigquery.sql_client import BigQuerySqlClient, BQ_TERMINAL_REASONS
from dlt.destinations.job_client_impl import SqlJobClientWithStaging
from dlt.destinations.job_impl import NewReferenceJob
from dlt.destinations.sql_jobs import SqlMergeJob
from dlt.destinations.type_mapping import TypeMapper
from dlt.pipeline.current import destination_state


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
        "wei": "BIGNUMERIC",  # non-parametrized should hold wei values
        "time": "TIME",
    }

    sct_to_dbt = {
        "text": "STRING(%i)",
        "binary": "BYTES(%i)",
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

    def to_db_decimal_type(self, precision: Optional[int], scale: Optional[int]) -> str:
        # Use BigQuery's BIGNUMERIC for large precision decimals
        precision, scale = self.decimal_precision(precision, scale)
        if precision > 38 or scale > 9:
            return "BIGNUMERIC(%i,%i)" % (precision, scale)
        return "NUMERIC(%i,%i)" % (precision, scale)

    # noinspection PyTypeChecker,PydanticTypeChecker
    def from_db_type(
        self, db_type: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        if db_type == "BIGNUMERIC" and precision is None:
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
        if not self.bq_load_job.done(retry=self.default_retry, timeout=self.http_timeout):
            return "running"
        if self.bq_load_job.output_rows is not None and self.bq_load_job.error_result is None:
            return "completed"
        reason = self.bq_load_job.error_result.get("reason")
        if reason in BQ_TERMINAL_REASONS:
            # the job permanently failed for the reason above
            return "failed"
        elif reason in ["internalError"]:
            logger.warning(
                f"Got reason {reason} for job {self.file_name}, job considered still"
                f" running. ({self.bq_load_job.error_result})"
            )
            # the status of the job couldn't be obtained, job still running.
            return "running"
        else:
            # retry on all other reasons, including `backendError` which requires retry when the job is done.
            return "retry"

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
        sql: List[str] = [
            f"FROM {root_table_name} AS d WHERE EXISTS (SELECT 1 FROM {staging_root_table_name} AS"
            f" s WHERE {clause.format(d='d', s='s')})"
            for clause in key_clauses
        ]
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

    def restore_file_load(self, file_path: str) -> LoadJob:
        """Returns a completed SqlLoadJob or restored BigQueryLoadJob

        See base class for details on SqlLoadJob.
        BigQueryLoadJob is restored with a job ID derived from `file_path`.

        Args:
            file_path (str): a path to a job file.

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
                    raise LoadJobNotExistsException(file_path) from gace
                elif reason in BQ_TERMINAL_REASONS:
                    raise LoadJobTerminalException(
                        file_path, f"The server reason was: {reason}"
                    ) from gace
                else:
                    raise DestinationTransientException(gace) from gace
        return job

    def start_file_load(self, table: TTableSchema, file_path: str, load_id: str) -> LoadJob:
        job = super().start_file_load(table, file_path, load_id)

        if not job:
            insert_api = table.get("x-insert-api", "default")
            try:
                if insert_api == "streaming":
                    if table["write_disposition"] != "append":
                        raise DestinationTerminalException(
                            "BigQuery streaming insert can only be used with `append`"
                            " write_disposition, while the given resource has"
                            f" `{table['write_disposition']}`."
                        )
                    if file_path.endswith(".jsonl"):
                        job_cls = DestinationJsonlLoadJob
                    elif file_path.endswith(".parquet"):
                        job_cls = DestinationParquetLoadJob  # type: ignore
                    else:
                        raise ValueError(
                            f"Unsupported file type for BigQuery streaming inserts: {file_path}"
                        )

                    job = job_cls(
                        table,
                        file_path,
                        self.config,  # type: ignore
                        self.schema,
                        destination_state(),
                        functools.partial(_streaming_load, self.sql_client),
                        [],
                    )
                else:
                    job = BigQueryLoadJob(
                        FileStorage.get_file_name_from_file_path(file_path),
                        self._create_load_job(table, file_path),
                        self.config.http_timeout,
                        self.config.retry_deadline,
                    )
            except api_core_exceptions.GoogleAPICallError as gace:
                reason = BigQuerySqlClient._get_reason_from_errors(gace)
                if reason == "notFound":
                    # google.api_core.exceptions.NotFound: 404 – table not found
                    raise UnknownTableException(table["name"]) from gace
                elif (
                    reason == "duplicate"
                ):  # google.api_core.exceptions.Conflict: 409 PUT – already exists
                    return self.restore_file_load(file_path)
                elif reason in BQ_TERMINAL_REASONS:
                    # google.api_core.exceptions.BadRequest - will not be processed ie bad job name
                    raise LoadJobTerminalException(
                        file_path, f"The server reason was: {reason}"
                    ) from gace
                else:
                    raise DestinationTransientException(gace) from gace

        return job

    def _get_table_update_sql(
        self, table_name: str, new_columns: Sequence[TColumnSchema], generate_alter: bool
    ) -> List[str]:
        table: Optional[TTableSchema] = self.prepare_load_table(table_name)
        sql = super()._get_table_update_sql(table_name, new_columns, generate_alter)
        canonical_name = self.sql_client.make_qualified_table_name(table_name)

        if partition_list := [
            c for c in new_columns if c.get("partition") or c.get(PARTITION_HINT, False)
        ]:
            if len(partition_list) > 1:
                col_names = [self.capabilities.escape_identifier(c["name"]) for c in partition_list]
                raise DestinationSchemaWillNotUpdate(
                    canonical_name, col_names, "Partition requested for more than one column"
                )
            elif (c := partition_list[0])["data_type"] == "date":
                sql[0] += f"\nPARTITION BY {self.capabilities.escape_identifier(c['name'])}"
            elif (c := partition_list[0])["data_type"] == "timestamp":
                sql[0] = (
                    f"{sql[0]}\nPARTITION BY DATE({self.capabilities.escape_identifier(c['name'])})"
                )
            # Automatic partitioning of an INT64 type requires us to be prescriptive - we treat the column as a UNIX timestamp.
            # This is due to the bounds requirement of GENERATE_ARRAY function for partitioning.
            # The 10,000 partitions limit makes it infeasible to cover the entire `bigint` range.
            # The array bounds, with daily partitions (86400 seconds in a day), are somewhat arbitrarily chosen.
            # See: https://dlthub.com/devel/dlt-ecosystem/destinations/bigquery#supported-column-hints
            elif (c := partition_list[0])["data_type"] == "bigint":
                sql[0] += (
                    f"\nPARTITION BY RANGE_BUCKET({self.capabilities.escape_identifier(c['name'])},"
                    " GENERATE_ARRAY(-172800000, 691200000, 86400))"
                )

        if cluster_list := [
            self.capabilities.escape_identifier(c["name"])
            for c in new_columns
            if c.get("cluster") or c.get(CLUSTER_HINT, False)
        ]:
            sql[0] += "\nCLUSTER BY " + ", ".join(cluster_list)

        # Table options.
        table_options: DictStrAny = {
            "description": (
                f"'{table.get(TABLE_DESCRIPTION_HINT)}'"
                if table.get(TABLE_DESCRIPTION_HINT)
                else None
            ),
            "expiration_timestamp": (
                f"TIMESTAMP '{table.get(TABLE_EXPIRATION_HINT)}'"
                if table.get(TABLE_EXPIRATION_HINT)
                else None
            ),
        }
        if not any(table_options.values()):
            return sql

        if generate_alter:
            logger.info(
                f"Table options for {table_name} are not applied on ALTER TABLE. Make sure that you"
                " set the table options ie. by using bigquery_adapter, before it is created."
            )
        else:
            sql[0] += (
                "\nOPTIONS ("
                + ", ".join(
                    [f"{key}={value}" for key, value in table_options.items() if value is not None]
                )
                + ")"
            )

        return sql

    def prepare_load_table(
        self, table_name: str, prepare_for_staging: bool = False
    ) -> Optional[TTableSchema]:
        table = super().prepare_load_table(table_name, prepare_for_staging)
        if table_name in self.schema.data_table_names():
            if TABLE_DESCRIPTION_HINT not in table:
                table[TABLE_DESCRIPTION_HINT] = (  # type: ignore[name-defined, typeddict-unknown-key, unused-ignore]
                    get_inherited_table_hint(
                        self.schema.tables, table_name, TABLE_DESCRIPTION_HINT, allow_none=True
                    )
                )
        return table

    def _get_column_def_sql(self, column: TColumnSchema, table_format: TTableFormat = None) -> str:
        name = self.capabilities.escape_identifier(column["name"])
        column_def_sql = (
            f"{name} {self.type_mapper.to_db_type(column, table_format)} {self._gen_not_null(column.get('nullable', True))}"
        )
        if column.get(ROUND_HALF_EVEN_HINT, False):
            column_def_sql += " OPTIONS (rounding_mode='ROUND_HALF_EVEN')"
        if column.get(ROUND_HALF_AWAY_FROM_ZERO_HINT, False):
            column_def_sql += " OPTIONS (rounding_mode='ROUND_HALF_AWAY_FROM_ZERO')"
        return column_def_sql

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
        # append to table for merge loads (append to stage) and regular appends.
        table_name = table["name"]

        # determine whether we load from local or uri
        bucket_path = None
        ext: str = os.path.splitext(file_path)[1][1:]
        if NewReferenceJob.is_reference_job(file_path):
            bucket_path = NewReferenceJob.resolve_reference(file_path)
            ext = os.path.splitext(bucket_path)[1][1:]

        # Select a correct source format
        source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        decimal_target_types: Optional[List[str]] = None
        if ext == "parquet":
            # if table contains complex types, we cannot load with parquet
            if table_schema_has_type(table, "complex"):
                raise LoadJobTerminalException(
                    file_path,
                    "Bigquery cannot load into JSON data type from parquet. Use jsonl instead.",
                )
            source_format = bigquery.SourceFormat.PARQUET
            # parquet needs NUMERIC type auto-detection
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


def _streaming_load(
    sql_client: SqlClientBase[BigQueryClient], items: List[Dict[Any, Any]], table: Dict[str, Any]
) -> None:
    """
    Upload the given items into BigQuery table, using streaming API.
    Streaming API is used for small amounts of data, with optimal
    batch size equal to 500 rows.

    Args:
        sql_client (dlt.destinations.impl.bigquery.bigquery.BigQueryClient):
            BigQuery client.
        items (List[Dict[Any, Any]]): List of rows to upload.
        table (Dict[Any, Any]): Table schema.
    """

    def _should_retry(exc: api_core_exceptions.GoogleAPICallError) -> bool:
        """Predicate to decide if we need to retry the exception.

        Args:
            exc (google.api_core.exceptions.GoogleAPICallError):
                Exception raised by the client.

        Returns:
            bool: True if the exception is retryable, False otherwise.
        """
        reason = exc.errors[0]["reason"]
        return reason in _RETRYABLE_REASONS

    full_name = sql_client.make_qualified_table_name(table["name"], escape=False)

    bq_client = sql_client._client
    bq_client.insert_rows_json(
        full_name,
        items,
        retry=retry.Retry(predicate=_should_retry, deadline=600),  # with 10 mins deadline
    )
