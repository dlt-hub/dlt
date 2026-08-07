import time
from typing import TYPE_CHECKING, Dict, Iterator, Optional, Sequence, List, Any

from dlt.common import logger
from dlt.common.destination.client import (
    FollowupJobRequest,
    HasFollowupJobs,
    LoadJob,
    PreparedTableSchema,
    RunnableLoadJob,
)
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.common.schema import TColumnSchema, TColumnHint, Schema
from dlt.common.schema.typing import TColumnType

from dlt.common.storages.load_package import ParsedLoadJobFileName
from dlt.destinations.sql_jobs import SqlStagingReplaceFollowupJob, SqlMergeFollowupJob

from dlt.destinations.insert_job_client import InsertValuesJobClient

from dlt.destinations.impl.mssql.sql_client import PyOdbcMsSqlClient
from dlt.destinations.impl.mssql.configuration import MsSqlClientConfiguration
from dlt.destinations.sql_client import SqlClientBase

if TYPE_CHECKING:
    from dlt.common.libs.pyarrow import pyarrow


HINT_TO_MSSQL_ATTR: Dict[TColumnHint, str] = {"unique": "UNIQUE"}
VARCHAR_MAX_N: int = 4000
VARBINARY_MAX_N: int = 8000


class MsSqlStagingReplaceJob(SqlStagingReplaceFollowupJob):
    @classmethod
    def generate_sql(
        cls,
        table_chain: Sequence[PreparedTableSchema],
        sql_client: SqlClientBase[Any],
    ) -> List[str]:
        sql: List[str] = []
        for table in table_chain:
            with sql_client.with_staging_dataset():
                staging_table_name = sql_client.make_qualified_table_name(table["name"])
            table_name = sql_client.make_qualified_table_name(table["name"])
            # drop destination table
            sql.append(f"DROP TABLE IF EXISTS {table_name}")
            # moving staging table to destination schema
            sql.append(
                f"ALTER SCHEMA {sql_client.fully_qualified_dataset_name()} TRANSFER"
                f" {staging_table_name}"
            )
            # recreate staging table
            sql.append(f"SELECT * INTO {staging_table_name} FROM {table_name} WHERE 1 = 0")
        return sql


class MsSqlMergeJob(SqlMergeFollowupJob):
    @classmethod
    def gen_key_table_clauses(
        cls,
        root_table_name: str,
        staging_root_table_name: str,
        primary_keys: Sequence[str],
        merge_keys: Sequence[str],
        for_delete: bool,
    ) -> List[str]:
        """Generate sql clauses that may be used to select or delete rows in root table of destination dataset"""
        if for_delete:
            # MS SQL doesn't support alias in DELETE FROM
            key_clauses = cls._gen_key_table_clauses(primary_keys, merge_keys)
            return [
                f"FROM {root_table_name} WHERE EXISTS (SELECT 1 FROM"
                f" {staging_root_table_name} WHERE"
                f" {' OR '.join([c.format(d=root_table_name,s=staging_root_table_name) for c in key_clauses])})"
            ]
        return SqlMergeFollowupJob.gen_key_table_clauses(
            root_table_name, staging_root_table_name, primary_keys, merge_keys, for_delete
        )

    @classmethod
    def _to_temp_table(
        cls,
        select_sql: str,
        temp_table_name: str,
        unique_column: str,
        sql_client: SqlClientBase[Any],
    ) -> str:
        return f"SELECT * INTO {temp_table_name} FROM ({select_sql}) as t"

    @classmethod
    def _new_temp_table_name(cls, table_name: str, op: str, sql_client: SqlClientBase[Any]) -> str:
        return SqlMergeFollowupJob._new_temp_table_name("#" + table_name, op, sql_client)


class MsSqlBulkCopyArrowJob(RunnableLoadJob, HasFollowupJobs):
    """Streams a parquet load file into its table with mssql-python's native Arrow bulk copy."""

    def __init__(self, file_path: str) -> None:
        super().__init__(file_path)
        self._job_client: MsSqlJobClient = None

    def run(self) -> None:
        from dlt.common.libs.pyarrow import pyarrow, pq_stream_with_new_columns

        config = self._job_client.config
        # bulk copy re-authenticates on a native connection of its own. It mints a fresh token from
        # `token_provider` or from `Authentication=`, but never reads the `attrs_before` struct that
        # carries a pre-acquired `access_token`, so it would sign in with no credential at all.
        if config.credentials.access_token:
            raise DestinationTerminalException(
                "`access_token` cannot be combined with the parquet loader file format on mssql:"
                " the native Arrow bulk copy opens its own connection and only re-acquires a token"
                " from `azure_credential` or `authentication`. Configure one of those instead, or"
                ' load with `loader_file_format="insert_values"`.'
            )

        sql_client = self._job_client.sql_client
        qualified_table_name = sql_client.make_qualified_table_name(self.load_table_name)

        with pyarrow.parquet.ParquetFile(self._file_path) as parquet_file:
            arrow_schema = parquet_file.schema_arrow
            num_rows = parquet_file.metadata.num_rows

        if num_rows == 0:
            logger.info(f"{self._file_name} is empty, skipping bulk copy to {qualified_table_name}")
            return

        # map explicitly instead of letting bulk copy align on ordinal position: a column added by a
        # later `ALTER TABLE` sits at the end of the table but keeps its schema order in the file
        column_mappings = [
            (index, sql_client.escape_column_name(name, quote=False))
            for index, name in enumerate(arrow_schema.names)
        ]

        def _iter_batches() -> Iterator["pyarrow.RecordBatch"]:
            for table in pq_stream_with_new_columns(self._file_path, ()):
                yield from table.to_batches()

        t_ = time.monotonic()
        cursor = sql_client.native_connection.cursor()
        try:
            result = cursor.bulkcopy_arrow(
                qualified_table_name,
                pyarrow.RecordBatchReader.from_batches(arrow_schema, _iter_batches()),
                timeout=config.bulk_copy_timeout,
                column_mappings=column_mappings,
                keep_nulls=True,
            )
        except Exception as ex:
            # the driver commits in batches the server sizes, on a connection dlt cannot roll back,
            # so a failure may already have committed a prefix of the file. Fail terminally rather
            # than let dlt retry the job and duplicate those rows.
            raise DestinationTerminalException(
                f"Arrow bulk copy of {self._file_name} into {qualified_table_name} failed and may"
                " have committed part of the file. The job is not retried so the rows are not"
                " duplicated; inspect the table before loading again."
            ) from ex
        finally:
            cursor.close()

        logger.info(
            f"{result.get('rows_copied')} rows copied from {self._file_name} to"
            f" {qualified_table_name} in {time.monotonic() - t_} s"
        )


class MsSqlJobClient(InsertValuesJobClient):
    def __init__(
        self,
        schema: Schema,
        config: MsSqlClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        dataset_name, staging_dataset_name = InsertValuesJobClient.create_dataset_names(
            schema, config
        )
        sql_client = PyOdbcMsSqlClient(
            dataset_name,
            staging_dataset_name,
            config.credentials,
            capabilities,
        )
        super().__init__(schema, config, sql_client)
        self.config: MsSqlClientConfiguration = config
        self.sql_client = sql_client
        self.active_hints = HINT_TO_MSSQL_ATTR if self.config.create_indexes else {}
        self.type_mapper = capabilities.get_type_mapper()

    def create_load_job(
        self, table: PreparedTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        job = super().create_load_job(table, file_path, load_id, restore)
        if not job:
            parsed_file = ParsedLoadJobFileName.parse(file_path)
            if parsed_file.file_format == "parquet":
                job = MsSqlBulkCopyArrowJob(file_path)
        return job

    def _create_merge_followup_jobs(
        self, table_chain: Sequence[PreparedTableSchema]
    ) -> List[FollowupJobRequest]:
        return [MsSqlMergeJob.from_table_chain(table_chain, self.sql_client)]

    def _make_add_column_sql(
        self, new_columns: Sequence[TColumnSchema], table: PreparedTableSchema = None
    ) -> List[str]:
        # Override because mssql requires multiple columns in a single ADD COLUMN clause
        return ["ADD \n" + ",\n".join(self._get_column_def_sql(c, table) for c in new_columns)]

    def _get_column_def_sql(self, c: TColumnSchema, table: PreparedTableSchema = None) -> str:
        sc_type = c["data_type"]
        if sc_type == "text" and c.get("unique"):
            # MSSQL does not allow index on large TEXT columns
            db_type = "nvarchar(%i)" % (c.get("precision") or 900)
        else:
            db_type = self.type_mapper.to_destination_type(c, table)

        hints_str = self._get_column_hints_sql(c)
        column_name = self.sql_client.escape_column_name(c["name"])
        return f"{column_name} {db_type} {hints_str} {self._gen_not_null(c.get('nullable', True))}"

    def _create_replace_followup_jobs(
        self, table_chain: Sequence[PreparedTableSchema]
    ) -> List[FollowupJobRequest]:
        root_table = table_chain[0]
        if root_table["x-replace-strategy"] == "staging-optimized":  # type: ignore[typeddict-item]
            return [MsSqlStagingReplaceJob.from_table_chain(table_chain, self.sql_client)]
        return super()._create_replace_followup_jobs(table_chain)

    def _from_db_type(
        self, pq_t: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        return self.type_mapper.from_destination_type(pq_t, precision, scale)
