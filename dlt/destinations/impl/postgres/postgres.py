from typing import Dict, Iterator, Optional, Sequence, List, Any

from dlt.common import logger
from dlt.common.destination.configuration import CsvFormatConfiguration
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.exceptions import (
    DestinationInvalidFileFormat,
)
from dlt.common.destination.client import (
    HasFollowupJobs,
    PreparedTableSchema,
    RunnableLoadJob,
    FollowupJobRequest,
    LoadJob,
)
from dlt.common.schema import TColumnSchema, TColumnHint, Schema
from dlt.common.schema.typing import TColumnType
from dlt.common.schema.utils import is_nullable_column
from dlt.common.storages.file_storage import FileStorage
from dlt.common.storages.load_storage import ParsedLoadJobFileName
from dlt.destinations.impl.postgres.configuration import PostgresClientConfiguration
from dlt.destinations.impl.postgres.sql_client import Psycopg2SqlClient
from dlt.destinations.insert_job_client import InsertValuesJobClient
from dlt.destinations.sql_client import SqlClientBase
from dlt.destinations.sql_jobs import SqlStagingReplaceFollowupJob

HINT_TO_POSTGRES_ATTR: Dict[TColumnHint, str] = {"unique": "UNIQUE"}


class PostgresStagingReplaceJob(SqlStagingReplaceFollowupJob):
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
            sql.extend(
                (
                    f"DROP TABLE IF EXISTS {table_name}",
                    (
                        f"ALTER TABLE {staging_table_name} SET SCHEMA"
                        f" {sql_client.fully_qualified_dataset_name()}"
                    ),
                    f"CREATE TABLE {staging_table_name} (like {table_name} including all)",
                )
            )
        return sql


class PostgresParquetCopyJob(RunnableLoadJob, HasFollowupJobs):
    def __init__(self, file_path: str) -> None:
        super().__init__(file_path)
        self._job_client: PostgresClient = None

    def run(self) -> None:
        self._config = self._job_client.config

        from dlt.common.libs.pyarrow import pq_stream_with_new_columns
        from dlt.common.libs.pyarrow import pyarrow
        import adbc_driver_postgresql.dbapi as adbapi

        def _iter_batches(file_path: str) -> Iterator[pyarrow.RecordBatch]:
            for table in pq_stream_with_new_columns(file_path, ()):
                yield from table.to_batches()

        with (
            adbapi.connect(self._config.credentials.to_native_representation()) as conn,
            conn.cursor() as cur,
        ):
            rows = cur.adbc_ingest(
                self.load_table_name,
                _iter_batches(self._file_path),
                mode="append",
                db_schema_name=self._job_client.sql_client.fully_qualified_dataset_name(
                    quote=False
                ),
            )
            logger.info(f"{rows} rows copied from {self._file_name} to {self.load_table_name}")
            conn.commit()


class PostgresCsvCopyJob(RunnableLoadJob, HasFollowupJobs):
    def __init__(self, file_path: str) -> None:
        super().__init__(file_path)
        self._job_client: PostgresClient = None

    def run(self) -> None:
        self._config = self._job_client.config
        sql_client = self._job_client.sql_client
        csv_format = self._config.csv_format or CsvFormatConfiguration()
        table_name = self.load_table_name
        sep = csv_format.delimiter
        if csv_format.on_error_continue:
            logger.warning(
                f"When processing {self._file_path} on table {table_name} Postgres csv reader does"
                " not support on_error_continue"
            )

        with FileStorage.open_zipsafe_ro(self._file_path, "rb") as f:
            if csv_format.include_header:
                # all headers in first line
                headers_row: str = f.readline().decode(csv_format.encoding).strip()
                split_headers = headers_row.split(sep)
            else:
                # read first row to figure out the headers
                split_first_row: str = f.readline().decode(csv_format.encoding).strip().split(sep)
                split_headers = list(self._job_client.schema.get_table_columns(table_name).keys())
                if len(split_first_row) > len(split_headers):
                    raise DestinationInvalidFileFormat(
                        "postgres",
                        "csv",
                        self._file_path,
                        f"First row `{split_first_row}` has more columns than header"
                        f" `{split_headers}` in table `{table_name}`",
                    )
                if len(split_first_row) < len(split_headers):
                    logger.warning(
                        f"First row {split_first_row} has less columns than header"
                        f" `{split_headers}` in table `{table_name}`. We will not load data to"
                        " superfluous columns."
                    )
                    split_headers = split_headers[: len(split_first_row)]
                # stream the first row again
                f.seek(0)

            # normalized and quoted headers
            split_headers = [
                sql_client.escape_column_name(h.strip('"'), quote=True) for h in split_headers
            ]
            split_null_headers = []
            split_columns = []
            # detect columns with NULL to use in FORCE NULL
            # detect headers that are not in columns
            for col in self._job_client.schema.get_table_columns(table_name).values():
                norm_col = sql_client.escape_column_name(col["name"], quote=True)
                split_columns.append(norm_col)
                if norm_col in split_headers and is_nullable_column(col):
                    split_null_headers.append(norm_col)
            if split_unknown_headers := set(split_headers).difference(split_columns):
                raise DestinationInvalidFileFormat(
                    "postgres",
                    "csv",
                    self._file_path,
                    f"Following headers {split_unknown_headers} cannot be matched to columns"
                    f" {split_columns} of table {table_name}.",
                )

            # use comma to join
            headers = ",".join(split_headers)
            if split_null_headers:
                null_headers = f"FORCE_NULL({','.join(split_null_headers)}),"
            else:
                null_headers = ""

            qualified_table_name = sql_client.make_qualified_table_name(table_name)
            copy_sql = (
                f"COPY {qualified_table_name} ({headers}) FROM STDIN WITH (FORMAT CSV, DELIMITER"
                f" '{sep}', NULL '', {null_headers} ENCODING '{csv_format.encoding}')"
            )
            with sql_client.begin_transaction():
                with sql_client.native_connection.cursor() as cursor:
                    cursor.copy_expert(copy_sql, f, size=8192)


class PostgresClient(InsertValuesJobClient):
    def __init__(
        self,
        schema: Schema,
        config: PostgresClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        sql_client = Psycopg2SqlClient(
            config.normalize_dataset_name(schema),
            config.normalize_staging_dataset_name(schema),
            config.credentials,
            capabilities,
        )
        super().__init__(schema, config, sql_client)
        self.config: PostgresClientConfiguration = config
        self.sql_client: Psycopg2SqlClient = sql_client
        self.active_hints = HINT_TO_POSTGRES_ATTR if self.config.create_indexes else {}
        self.type_mapper = self.capabilities.get_type_mapper()

    def create_load_job(
        self, table: PreparedTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        job = super().create_load_job(table, file_path, load_id, restore)
        if not job:
            parsed_file = ParsedLoadJobFileName.parse(file_path)
            if parsed_file.file_format == "csv":
                job = PostgresCsvCopyJob(file_path)
            elif parsed_file.file_format == "parquet":
                job = PostgresParquetCopyJob(file_path)
        return job

    def _create_replace_followup_jobs(
        self, table_chain: Sequence[PreparedTableSchema]
    ) -> List[FollowupJobRequest]:
        root_table = table_chain[0]
        if root_table["x-replace-strategy"] == "staging-optimized":  # type: ignore[typeddict-item]
            return [PostgresStagingReplaceJob.from_table_chain(table_chain, self.sql_client)]
        return super()._create_replace_followup_jobs(table_chain)

    def _from_db_type(
        self, pq_t: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        return self.type_mapper.from_destination_type(pq_t, precision, scale)
