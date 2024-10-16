import contextlib
from typing import Dict, Optional, Sequence, List, Any, Iterator

from shapely import wkt, wkb
from shapely.geometry.base import BaseGeometry

from dlt.common import logger
from dlt.common.data_writers.configuration import CsvFormatConfiguration
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.exceptions import (
    DestinationInvalidFileFormat,
)
from dlt.common.destination.reference import (
    HasFollowupJobs,
    PreparedTableSchema,
    RunnableLoadJob,
    FollowupJobRequest,
    LoadJob,
)
from dlt.common.exceptions import TerminalValueError
from dlt.common.schema import TColumnSchema, TColumnHint, Schema
from dlt.common.schema.typing import TColumnType
from dlt.common.schema.utils import is_nullable_column
from dlt.common.storages.file_storage import FileStorage
from dlt.destinations.impl.postgres.configuration import PostgresClientConfiguration
from dlt.destinations.impl.postgres.postgres_adapter import GEOMETRY_HINT, SRID_HINT
from dlt.destinations.impl.postgres.sql_client import Psycopg2SqlClient
from dlt.destinations.insert_job_client import InsertValuesJobClient, InsertValuesLoadJob
from dlt.destinations.sql_client import SqlClientBase
from dlt.destinations.sql_jobs import SqlStagingCopyFollowupJob, SqlJobParams

HINT_TO_POSTGRES_ATTR: Dict[TColumnHint, str] = {"unique": "UNIQUE"}


class PostgresStagingCopyJob(SqlStagingCopyFollowupJob):
    @classmethod
    def generate_sql(
        cls,
        table_chain: Sequence[PreparedTableSchema],
        sql_client: SqlClientBase[Any],
        params: Optional[SqlJobParams] = None,
    ) -> List[str]:
        sql: List[str] = []
        for table in table_chain:
            with sql_client.with_staging_dataset():
                staging_table_name = sql_client.make_qualified_table_name(table["name"])
            table_name = sql_client.make_qualified_table_name(table["name"])
            # drop destination table
            sql.append(f"DROP TABLE IF EXISTS {table_name};")
            # moving staging table to destination schema
            sql.append(
                f"ALTER TABLE {staging_table_name} SET SCHEMA"
                f" {sql_client.fully_qualified_dataset_name()};"
            )
            # recreate staging table
            sql.append(f"CREATE TABLE {staging_table_name} (like {table_name} including all);")
        return sql


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
                        f"First row {split_first_row} has more rows than columns {split_headers} in"
                        f" table {table_name}",
                    )
                if len(split_first_row) < len(split_headers):
                    logger.warning(
                        f"First row {split_first_row} has less rows than columns {split_headers} in"
                        f" table {table_name}. We will not load data to superfluous columns."
                    )
                    split_headers = split_headers[: len(split_first_row)]
                # stream the first row again
                f.seek(0)

            # normalized and quoted headers
            split_headers = [
                sql_client.escape_column_name(h.strip('"'), escape=True) for h in split_headers
            ]
            split_null_headers = []
            split_columns = []
            # detect columns with NULL to use in FORCE NULL
            # detect headers that are not in columns
            for col in self._job_client.schema.get_table_columns(table_name).values():
                norm_col = sql_client.escape_column_name(col["name"], escape=True)
                split_columns.append(norm_col)
                if norm_col in split_headers and is_nullable_column(col):
                    split_null_headers.append(norm_col)
            split_unknown_headers = set(split_headers).difference(split_columns)
            if split_unknown_headers:
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
                "COPY %s (%s) FROM STDIN WITH (FORMAT CSV, DELIMITER '%s', NULL '',"
                " %s ENCODING '%s')"
                % (
                    qualified_table_name,
                    headers,
                    sep,
                    null_headers,
                    csv_format.encoding,
                )
            )
            with sql_client.begin_transaction():
                with sql_client.native_connection.cursor() as cursor:
                    cursor.copy_expert(copy_sql, f, size=8192)


class PostgresInsertValuesWithGeometryTypesLoadJob(InsertValuesLoadJob):
    def __init__(self, file_path: str, postgres_client: "PostgresClient") -> None:
        super().__init__(file_path)
        self._postgres_client = postgres_client

    @staticmethod
    def _parse_geometry(value: Any) -> Optional[str]:
        if isinstance(value, (str, bytes)):
            with contextlib.suppress(Exception):
                geom = wkt.loads(value) if isinstance(value, str) else wkb.loads(value)
                if isinstance(geom, BaseGeometry):
                    return geom.wkb_hex
        return None

    def _insert(self, qualified_table_name: str, file_path: str) -> Iterator[List[str]]:
        with FileStorage.open_zipsafe_ro(file_path, "r", encoding="utf-8") as f:
            header = f.readline()
            header = self._sql_client.capabilities.casefold_identifier(header).format(
                qualified_table_name
            )
            values_mark = f.readline()
            assert values_mark == "VALUES\n"
            insert_sql = []
            while content := f.read(self._sql_client.capabilities.max_query_length // 2):
                until_nl = f.readline().strip("\n")
                is_eof = len(until_nl) == 0 or until_nl[-1] == ";"
                if not is_eof:
                    until_nl = f"{until_nl[:-1]};"
                values_rows = content.splitlines(keepends=True)
                processed_rows = []
                for row in values_rows:
                    row_values = row.strip().strip("(),").split(",")
                    processed_values = []
                    for idx, value in enumerate(row_values):
                        column = self._load_table["columns"][
                            list(self._load_table["columns"].keys())[idx]
                        ]
                        if column.get(GEOMETRY_HINT):
                            if geom_value := self._parse_geometry(value.strip()):
                                srid = column.get(SRID_HINT, 4326)
                                processed_values.append(
                                    f"ST_SetSRID(ST_GeomFromWKB(decode('{geom_value}', 'hex')),"
                                    f" {srid})"
                                )
                            else:
                                processed_values.append(value)
                        else:
                            processed_values.append(value)
                    processed_rows.append(f"({','.join(processed_values)})")
                processed_content = ",\n".join(processed_rows)
                insert_sql.extend([header, values_mark, processed_content + until_nl])
                if not is_eof:
                    yield insert_sql
                    insert_sql = []
            if insert_sql:
                yield insert_sql


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
        if any(column.get(GEOMETRY_HINT) for column in table["columns"].values()):
            if file_path.endswith("insert_values"):
                return PostgresInsertValuesWithGeometryTypesLoadJob(file_path)
            else:
                # Only insert_values load jobs supported for geom types.
                raise TerminalValueError(
                    "CSV bulk loading is not supported for tables with geometry columns."
                )
        job = super().create_load_job(table, file_path, load_id, restore)
        if not job and file_path.endswith("csv"):
            job = PostgresCsvCopyJob(file_path)
        return job

    def _get_column_def_sql(self, c: TColumnSchema, table: PreparedTableSchema = None) -> str:
        hints_ = " ".join(
            self.active_hints.get(h, "")
            for h in self.active_hints.keys()
            if c.get(h, False) is True
        )
        column_name = self.sql_client.escape_column_name(c["name"])
        nullability = self._gen_not_null(c.get("nullable", True))

        if c.get(GEOMETRY_HINT):
            srid = c.get(SRID_HINT, 4326)
            column_type = f"geometry(Geometry, {srid})"
        else:
            column_type = self.type_mapper.to_destination_type(c, table)

        return f"{column_name} {column_type} {hints_} {nullability}"

    def _create_replace_followup_jobs(
        self, table_chain: Sequence[PreparedTableSchema]
    ) -> List[FollowupJobRequest]:
        if self.config.replace_strategy == "staging-optimized":
            return [PostgresStagingCopyJob.from_table_chain(table_chain, self.sql_client)]
        return super()._create_replace_followup_jobs(table_chain)

    def _from_db_type(
        self, pq_t: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        return self.type_mapper.from_destination_type(pq_t, precision, scale)
