import functools
import os
from pathlib import Path
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, cast

import google.cloud.bigquery as bigquery  # noqa: I250
from google.api_core import exceptions as api_core_exceptions
from google.cloud import exceptions as gcp_exceptions
from google.api_core import retry
from google.cloud.bigquery.retry import _RETRYABLE_REASONS

from dlt.common import logger
from dlt.common.runtime.signals import sleep
from dlt.common.json import json
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import (
    HasFollowupJobs,
    FollowupJobRequest,
    TLoadJobState,
    RunnableLoadJob,
    SupportsStagingDestination,
    LoadJob,
)
from dlt.common.schema import TColumnSchema, Schema, TTableSchemaColumns
from dlt.common.schema.typing import TTableSchema, TColumnType, TTableFormat
from dlt.common.schema.utils import get_inherited_table_hint
from dlt.common.schema.utils import table_schema_has_type
from dlt.common.storages.file_storage import FileStorage
from dlt.common.storages.load_package import destination_state
from dlt.common.typing import DictStrAny
from dlt.destinations.job_impl import DestinationJsonlLoadJob, DestinationParquetLoadJob
from dlt.destinations.sql_client import SqlClientBase
from dlt.destinations.exceptions import (
    DatabaseTransientException,
    DatabaseUndefinedRelation,
    DestinationSchemaWillNotUpdate,
    DestinationTerminalException,
    DatabaseTerminalException,
    LoadJobTerminalException,
)
from dlt.destinations.impl.bigquery.bigquery_adapter import (
    AUTODETECT_SCHEMA_HINT,
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
from dlt.destinations.job_impl import ReferenceFollowupJobRequest
from dlt.destinations.sql_jobs import SqlMergeFollowupJob
from dlt.destinations.type_mapping import TypeMapper
from dlt.destinations.utils import parse_db_data_type_str_with_precision


class BigQueryTypeMapper(TypeMapper):
    sct_to_unbound_dbt = {
        "complex": "JSON",
        "text": "STRING",
        "double": "FLOAT64",
        "bool": "BOOL",
        "date": "DATE",
        "timestamp": "TIMESTAMP",
        "bigint": "INT64",
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
        "FLOAT64": "double",
        "BOOL": "bool",
        "DATE": "date",
        "TIMESTAMP": "timestamp",
        "INT64": "bigint",
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
        # precision is present in the type name
        if db_type == "BIGNUMERIC":
            return dict(data_type="wei")
        return super().from_db_type(*parse_db_data_type_str_with_precision(db_type))


class BigQueryLoadJob(RunnableLoadJob, HasFollowupJobs):
    def __init__(
        self,
        file_path: str,
        http_timeout: float,
        retry_deadline: float,
    ) -> None:
        super().__init__(file_path)
        self._default_retry = bigquery.DEFAULT_RETRY.with_deadline(retry_deadline)
        self._http_timeout = http_timeout
        self._job_client: "BigQueryClient" = None
        self._bq_load_job: bigquery.LoadJob = None
        # vars only used for testing
        self._created_job = False
        self._resumed_job = False

    def run(self) -> None:
        # start the job (or retrieve in case it already exists)
        try:
            self._bq_load_job = self._job_client._create_load_job(self._load_table, self._file_path)
            self._created_job = True
        except api_core_exceptions.GoogleAPICallError as gace:
            reason = BigQuerySqlClient._get_reason_from_errors(gace)
            if reason == "notFound":
                # google.api_core.exceptions.NotFound: 404 – table not found
                raise DatabaseUndefinedRelation(gace) from gace
            elif (
                reason == "duplicate"
            ):  # google.api_core.exceptions.Conflict: 409 PUT – already exists
                self._bq_load_job = self._job_client._retrieve_load_job(self._file_path)
                self._resumed_job = True
                logger.info(
                    f"Found existing bigquery job for job {self._file_name}, will resume job."
                )
            elif reason in BQ_TERMINAL_REASONS:
                # google.api_core.exceptions.BadRequest - will not be processed ie bad job name
                raise LoadJobTerminalException(
                    self._file_path, f"The server reason was: {reason}"
                ) from gace
            else:
                raise DatabaseTransientException(gace) from gace

        # we loop on the job thread until we detect a status change
        while True:
            sleep(1)
            # not done yet
            if not self._bq_load_job.done(retry=self._default_retry, timeout=self._http_timeout):
                continue
            # done, break loop and go to completed state
            if self._bq_load_job.output_rows is not None and self._bq_load_job.error_result is None:
                break
            reason = self._bq_load_job.error_result.get("reason")
            if reason in BQ_TERMINAL_REASONS:
                # the job permanently failed for the reason above
                raise DatabaseTerminalException(
                    Exception(
                        f"Bigquery Load Job failed, reason reported from bigquery: '{reason}'"
                    )
                )
            elif reason in ["internalError"]:
                logger.warning(
                    f"Got reason {reason} for job {self._file_name}, job considered still"
                    f" running. ({self._bq_load_job.error_result})"
                )
                continue
            else:
                raise DatabaseTransientException(
                    Exception(
                        f"Bigquery Job needs to be retried, reason reported from bigquer '{reason}'"
                    )
                )

    def exception(self) -> str:
        return json.dumps(
            {
                "error_result": self._bq_load_job.error_result,
                "errors": self._bq_load_job.errors,
                "job_start": self._bq_load_job.started,
                "job_end": self._bq_load_job.ended,
                "job_id": self._bq_load_job.job_id,
            }
        )

    @staticmethod
    def get_job_id_from_file_path(file_path: str) -> str:
        return Path(file_path).name.replace(".", "_")


class BigQueryMergeJob(SqlMergeFollowupJob):
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
    def __init__(
        self,
        schema: Schema,
        config: BigQueryClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        sql_client = BigQuerySqlClient(
            config.normalize_dataset_name(schema),
            config.normalize_staging_dataset_name(schema),
            config.credentials,
            capabilities,
            config.get_location(),
            config.project_id,
            config.http_timeout,
            config.retry_deadline,
        )
        super().__init__(schema, config, sql_client)
        self.config: BigQueryClientConfiguration = config
        self.sql_client: BigQuerySqlClient = sql_client  # type: ignore
        self.type_mapper = BigQueryTypeMapper(self.capabilities)

    def _create_merge_followup_jobs(
        self, table_chain: Sequence[TTableSchema]
    ) -> List[FollowupJobRequest]:
        return [BigQueryMergeJob.from_table_chain(table_chain, self.sql_client)]

    def create_load_job(
        self, table: TTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        job = super().create_load_job(table, file_path, load_id)

        if not job:
            insert_api = table.get("x-insert-api", "default")
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
                    file_path,
                    self.config,  # type: ignore
                    destination_state(),
                    _streaming_load,  # type: ignore
                    [],
                    callable_requires_job_client_args=True,
                )
            else:
                job = BigQueryLoadJob(
                    file_path,
                    self.config.http_timeout,
                    self.config.retry_deadline,
                )
        return job

    def _get_table_update_sql(
        self, table_name: str, new_columns: Sequence[TColumnSchema], generate_alter: bool
    ) -> List[str]:
        # return empty columns which will skip table CREATE or ALTER
        # to let BigQuery autodetect table from data
        if self._should_autodetect_schema(table_name):
            return []

        table: Optional[TTableSchema] = self.prepare_load_table(table_name)
        sql = super()._get_table_update_sql(table_name, new_columns, generate_alter)
        canonical_name = self.sql_client.make_qualified_table_name(table_name)

        if partition_list := [
            c for c in new_columns if c.get("partition") or c.get(PARTITION_HINT, False)
        ]:
            if len(partition_list) > 1:
                col_names = [self.sql_client.escape_column_name(c["name"]) for c in partition_list]
                raise DestinationSchemaWillNotUpdate(
                    canonical_name, col_names, "Partition requested for more than one column"
                )
            elif (c := partition_list[0])["data_type"] == "date":
                sql[0] += f"\nPARTITION BY {self.sql_client.escape_column_name(c['name'])}"
            elif (c := partition_list[0])["data_type"] == "timestamp":
                sql[0] = (
                    f"{sql[0]}\nPARTITION BY DATE({self.sql_client.escape_column_name(c['name'])})"
                )
            # Automatic partitioning of an INT64 type requires us to be prescriptive - we treat the column as a UNIX timestamp.
            # This is due to the bounds requirement of GENERATE_ARRAY function for partitioning.
            # The 10,000 partitions limit makes it infeasible to cover the entire `bigint` range.
            # The array bounds, with daily partitions (86400 seconds in a day), are somewhat arbitrarily chosen.
            # See: https://dlthub.com/devel/dlt-ecosystem/destinations/bigquery#supported-column-hints
            elif (c := partition_list[0])["data_type"] == "bigint":
                sql[0] += (
                    f"\nPARTITION BY RANGE_BUCKET({self.sql_client.escape_column_name(c['name'])},"
                    " GENERATE_ARRAY(-172800000, 691200000, 86400))"
                )

        if cluster_list := [
            self.sql_client.escape_column_name(c["name"])
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

    def get_storage_tables(
        self, table_names: Iterable[str]
    ) -> Iterable[Tuple[str, TTableSchemaColumns]]:
        """Gets table schemas from BigQuery using INFORMATION_SCHEMA or get_table for hidden datasets"""
        if not self.sql_client.is_hidden_dataset:
            return super().get_storage_tables(table_names)

        # use the api to get storage tables for hidden dataset
        schema_tables: List[Tuple[str, TTableSchemaColumns]] = []
        for table_name in table_names:
            try:
                schema_table: TTableSchemaColumns = {}
                table = self.sql_client.native_connection.get_table(
                    self.sql_client.make_qualified_table_name(table_name, escape=False),
                    retry=self.sql_client._default_retry,
                    timeout=self.config.http_timeout,
                )
                for c in table.schema:
                    schema_c: TColumnSchema = {
                        "name": c.name,
                        "nullable": c.is_nullable,
                        **self._from_db_type(c.field_type, c.precision, c.scale),
                    }
                    schema_table[c.name] = schema_c
                schema_tables.append((table_name, schema_table))
            except gcp_exceptions.NotFound:
                # table is not present
                schema_tables.append((table_name, {}))
        return schema_tables

    def _get_info_schema_columns_query(
        self, catalog_name: Optional[str], schema_name: str, folded_table_names: List[str]
    ) -> Tuple[str, List[Any]]:
        """Bigquery needs to scope the INFORMATION_SCHEMA.COLUMNS with project and dataset name so standard query generator cannot be used."""
        # escape schema and catalog names
        catalog_name = self.capabilities.escape_identifier(catalog_name)
        schema_name = self.capabilities.escape_identifier(schema_name)

        query = f"""
SELECT {",".join(self._get_storage_table_query_columns())}
    FROM {catalog_name}.{schema_name}.INFORMATION_SCHEMA.COLUMNS
"""
        if folded_table_names:
            # placeholder for each table
            table_placeholders = ",".join(["%s"] * len(folded_table_names))
            query += f"WHERE table_name IN ({table_placeholders}) "
        query += "ORDER BY table_name, ordinal_position;"

        return query, folded_table_names

    def _get_column_def_sql(self, column: TColumnSchema, table_format: TTableFormat = None) -> str:
        name = self.sql_client.escape_column_name(column["name"])
        column_def_sql = (
            f"{name} {self.type_mapper.to_db_type(column, table_format)} {self._gen_not_null(column.get('nullable', True))}"
        )
        if column.get(ROUND_HALF_EVEN_HINT, False):
            column_def_sql += " OPTIONS (rounding_mode='ROUND_HALF_EVEN')"
        if column.get(ROUND_HALF_AWAY_FROM_ZERO_HINT, False):
            column_def_sql += " OPTIONS (rounding_mode='ROUND_HALF_AWAY_FROM_ZERO')"
        return column_def_sql

    def _create_load_job(self, table: TTableSchema, file_path: str) -> bigquery.LoadJob:
        # append to table for merge loads (append to stage) and regular appends.
        table_name = table["name"]

        # determine whether we load from local or url
        bucket_path = None
        ext: str = os.path.splitext(file_path)[1][1:]
        if ReferenceFollowupJobRequest.is_reference_job(file_path):
            bucket_path = ReferenceFollowupJobRequest.resolve_reference(file_path)
            ext = os.path.splitext(bucket_path)[1][1:]

        # Select a correct source format
        source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        decimal_target_types: Optional[List[str]] = None
        if ext == "parquet":
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
        if self._should_autodetect_schema(table_name):
            # allow BigQuery to infer and evolve the schema, note that dlt is not
            # creating such tables at all
            job_config.autodetect = True
            job_config.schema_update_options = bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
            job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
        elif ext == "parquet" and table_schema_has_type(table, "complex"):
            # if table contains complex types, we cannot load with parquet
            raise LoadJobTerminalException(
                file_path,
                "Bigquery cannot load into JSON data type from parquet. Enable autodetect_schema in"
                " config or via BigQuery adapter or use jsonl format instead.",
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

    def _should_autodetect_schema(self, table_name: str) -> bool:
        return get_inherited_table_hint(
            self.schema._schema_tables, table_name, AUTODETECT_SCHEMA_HINT, allow_none=True
        ) or (self.config.autodetect_schema and table_name not in self.schema.dlt_table_names())

    def should_truncate_table_before_load_on_staging_destination(self, table: TTableSchema) -> bool:
        return self.config.truncate_tables_on_staging_destination_before_load


def _streaming_load(
    items: List[Dict[Any, Any]], table: Dict[str, Any], job_client: BigQueryClient
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

    sql_client = job_client.sql_client

    full_name = sql_client.make_qualified_table_name(table["name"], escape=False)

    bq_client = sql_client._client
    bq_client.insert_rows_json(
        full_name,
        items,
        retry=retry.Retry(predicate=_should_retry, deadline=600),  # with 10 mins deadline
    )
