from typing import Dict, Optional, Sequence, List, Any

from dlt.common import logger
from dlt.common.data_writers.configuration import CsvFormatConfiguration
from dlt.common.destination.exceptions import (
    DestinationInvalidFileFormat,
    DestinationTerminalException,
)
from dlt.common.destination.reference import FollowupJob, LoadJob, NewLoadJob, TLoadJobState
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.exceptions import TerminalValueError
from dlt.common.schema import TColumnSchema, TColumnHint, Schema
from dlt.common.schema.typing import TTableSchema, TColumnType, TTableFormat
from dlt.common.storages.file_storage import FileStorage

from dlt.destinations.sql_jobs import SqlStagingCopyJob, SqlJobParams
from dlt.destinations.insert_job_client import InsertValuesJobClient
from dlt.destinations.impl.postgres.sql_client import Psycopg2SqlClient
from dlt.destinations.impl.postgres.configuration import PostgresClientConfiguration
from dlt.destinations.sql_client import SqlClientBase
from dlt.destinations.type_mapping import TypeMapper

HINT_TO_POSTGRES_ATTR: Dict[TColumnHint, str] = {"unique": "UNIQUE"}


class PostgresTypeMapper(TypeMapper):
    sct_to_unbound_dbt = {
        "complex": "jsonb",
        "text": "varchar",
        "double": "double precision",
        "bool": "boolean",
        "date": "date",
        "bigint": "bigint",
        "binary": "bytea",
        "timestamp": "timestamp with time zone",
        "time": "time without time zone",
    }

    sct_to_dbt = {
        "text": "varchar(%i)",
        "timestamp": "timestamp (%i) with time zone",
        "decimal": "numeric(%i,%i)",
        "time": "time (%i) without time zone",
        "wei": "numeric(%i,%i)",
    }

    dbt_to_sct = {
        "varchar": "text",
        "jsonb": "complex",
        "double precision": "double",
        "boolean": "bool",
        "timestamp with time zone": "timestamp",
        "date": "date",
        "bigint": "bigint",
        "bytea": "binary",
        "numeric": "decimal",
        "time without time zone": "time",
        "character varying": "text",
        "smallint": "bigint",
        "integer": "bigint",
    }

    def to_db_integer_type(
        self, precision: Optional[int], table_format: TTableFormat = None
    ) -> str:
        if precision is None:
            return "bigint"
        # Precision is number of bits
        if precision <= 16:
            return "smallint"
        elif precision <= 32:
            return "integer"
        elif precision <= 64:
            return "bigint"
        raise TerminalValueError(
            f"bigint with {precision} bits precision cannot be mapped into postgres integer type"
        )

    def from_db_type(
        self, db_type: str, precision: Optional[int] = None, scale: Optional[int] = None
    ) -> TColumnType:
        if db_type == "numeric":
            if (precision, scale) == self.capabilities.wei_precision:
                return dict(data_type="wei")
        return super().from_db_type(db_type, precision, scale)


class PostgresStagingCopyJob(SqlStagingCopyJob):
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
            # moving staging table to destination schema
            sql.append(
                f"ALTER TABLE {staging_table_name} SET SCHEMA"
                f" {sql_client.fully_qualified_dataset_name()};"
            )
            # recreate staging table
            sql.append(f"CREATE TABLE {staging_table_name} (like {table_name} including all);")
        return sql


class PostgresCsvCopyJob(LoadJob, FollowupJob):
    def __init__(self, table: TTableSchema, file_path: str, client: "PostgresClient") -> None:
        super().__init__(FileStorage.get_file_name_from_file_path(file_path))
        config = client.config
        sql_client = client.sql_client
        csv_format = config.csv_format or CsvFormatConfiguration()
        table_name = table["name"]
        sep = csv_format.delimiter
        if csv_format.on_error_continue:
            logger.warning(
                f"When processing {file_path} on table {table_name} Postgres csv reader does not"
                " support on_error_continue"
            )

        with FileStorage.open_zipsafe_ro(file_path, "rb") as f:
            if csv_format.include_header:
                # all headers in first line
                headers_row: str = f.readline().decode(csv_format.encoding).strip()
                split_headers = headers_row.split(sep)
            else:
                # read first row to figure out the headers
                split_first_row: str = f.readline().decode(csv_format.encoding).strip().split(sep)
                split_headers = list(client.schema.get_table_columns(table_name).keys())
                if len(split_first_row) > len(split_headers):
                    raise DestinationInvalidFileFormat(
                        "postgres",
                        "csv",
                        file_path,
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
            for col in client.schema.get_table_columns(table_name).values():
                norm_col = sql_client.escape_column_name(col["name"], escape=True)
                split_columns.append(norm_col)
                if norm_col in split_headers and col.get("nullable", True):
                    split_null_headers.append(norm_col)
            split_unknown_headers = set(split_headers).difference(split_columns)
            if split_unknown_headers:
                raise DestinationInvalidFileFormat(
                    "postgres",
                    "csv",
                    file_path,
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

    def state(self) -> TLoadJobState:
        return "completed"

    def exception(self) -> str:
        raise NotImplementedError()


class PostgresClient(InsertValuesJobClient):
    def __init__(
        self,
        schema: Schema,
        config: PostgresClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        sql_client = Psycopg2SqlClient(
            config.normalize_dataset_name(schema), config.credentials, capabilities
        )
        super().__init__(schema, config, sql_client)
        self.config: PostgresClientConfiguration = config
        self.sql_client: Psycopg2SqlClient = sql_client
        self.active_hints = HINT_TO_POSTGRES_ATTR if self.config.create_indexes else {}
        self.type_mapper = PostgresTypeMapper(self.capabilities)

    def start_file_load(self, table: TTableSchema, file_path: str, load_id: str) -> LoadJob:
        job = super().start_file_load(table, file_path, load_id)
        if not job and file_path.endswith("csv"):
            job = PostgresCsvCopyJob(table, file_path, self)
        return job

    def _get_column_def_sql(self, c: TColumnSchema, table_format: TTableFormat = None) -> str:
        hints_str = " ".join(
            self.active_hints.get(h, "")
            for h in self.active_hints.keys()
            if c.get(h, False) is True
        )
        column_name = self.sql_client.escape_column_name(c["name"])
        return (
            f"{column_name} {self.type_mapper.to_db_type(c)} {hints_str} {self._gen_not_null(c.get('nullable', True))}"
        )

    def _create_replace_followup_jobs(
        self, table_chain: Sequence[TTableSchema]
    ) -> List[NewLoadJob]:
        if self.config.replace_strategy == "staging-optimized":
            return [PostgresStagingCopyJob.from_table_chain(table_chain, self.sql_client)]
        return super()._create_replace_followup_jobs(table_chain)

    def _from_db_type(
        self, pq_t: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        return self.type_mapper.from_db_type(pq_t, precision, scale)
