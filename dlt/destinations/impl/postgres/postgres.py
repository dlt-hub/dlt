import contextlib
import re
from typing import Dict, Optional, Sequence, List, Any, Iterator, Tuple

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
    @staticmethod
    def _parse_geometry(value: Any) -> Optional[str]:
        if isinstance(value, (str, bytes)):
            with contextlib.suppress(Exception):
                geom = wkt.loads(value) if isinstance(value, str) else wkb.loads(value)
                if isinstance(geom, BaseGeometry):
                    return geom.wkb_hex
        return None

    @staticmethod
    def extract_records_from_fragment(fragment: str) -> List[Tuple[Any, ...]]:
        def parse_value(value: str) -> Any:
            value = value.strip()
            if value.startswith("E'") and value.endswith("'"):
                return bytes(value[2:-1], "utf-8").decode("unicode_escape")
            if value.startswith("'") and value.endswith("'"):
                return value[1:-1]
            if value.lower() in ('true', 'false'):
                return value.lower()=='true'
            if value.lower()=='null':
                return None
            try:
                return int(value)
            except ValueError:
                try:
                    return float(value)
                except ValueError:
                    return value

        def split_record(record: str) -> List[str]:
            fields, current_field, paren_level, in_quotes = [], [], 0, False
            escape = False
            for char in record:
                if escape:
                    current_field.append(char)
                    escape = False
                    continue
                if char=='\\':
                    escape = True
                    current_field.append(char)
                    continue
                if char=="'" and not in_quotes:
                    in_quotes = True
                elif char=="'" and in_quotes:
                    in_quotes = False
                elif char in '([{':
                    paren_level += 1
                elif char in ')]}':
                    paren_level -= 1
                elif char==',' and paren_level==0 and not in_quotes:
                    fields.append(''.join(current_field).strip())
                    current_field = []
                    continue
                current_field.append(char)
            fields.append(''.join(current_field).strip())
            return fields

            # Match top-level records using regex with non-greedy matching for nested structures

        record_strings = re.findall(r'\((.*?)\)(?=,|\Z)', fragment)
        records = [tuple(parse_value(field) for field in split_record(record)) for record in record_strings]
        return records

    def _insert(self, qualified_table_name: str, file_path: str) -> Iterator[List[str]]:
        to_insert = super()._insert(qualified_table_name, file_path)
        for fragments in to_insert:
            processed_fragments = []
            for fragment in fragments:
                if fragment == "VALUES\n" or fragment.strip().upper().startswith("INSERT INTO "):
                    processed_fragments.append(fragment)
                else:
                    columns = self._load_table["columns"]
                    processed_fragment_lines: List[str] = re.findall(
                        r"\(E'[^']+',E'[^']+',E'[^']+',E'[^']+'\)", fragment
                    )

                    for line in processed_fragment_lines:
                        processed_fragment: List[str] = re.findall(r"", fragment)
                        assert len(columns) == len(processed_fragments)

                        for column, column_value in zip(columns, processed_fragment):
                            if column.get(GEOMETRY_HINT):
                                if geom_value := self._parse_geometry(column_value.strip()):
                                    srid = column.get(SRID_HINT, 4326)
                                    processed_fragment.append(
                                        f"ST_SetSRID(ST_GeomFromWKB(decode('{geom_value}', 'hex')),"
                                        f" {srid})"
                                    )
                                else:
                                    logger.warning(
                                        f"{column_value} couldn't be coerced to geometric type."
                                    )
                                    processed_fragment.append(column_value)
                            else:
                                processed_fragment.append(column_value)

                        processed_fragments.append(processed_fragment)

            yield processed_fragments


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
