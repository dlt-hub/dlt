import threading
from typing import ClassVar, Dict, Optional

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.data_types import TDataType
from dlt.common.exceptions import TerminalValueError
from dlt.common.schema import TColumnSchema, TColumnHint, Schema
from dlt.common.destination.reference import RunnableLoadJob, HasFollowupJobs, LoadJob
from dlt.common.schema.typing import TTableSchema, TColumnType, TTableFormat
from dlt.common.storages.file_storage import FileStorage
from dlt.common.utils import maybe_context

from dlt.destinations.insert_job_client import InsertValuesJobClient

from dlt.destinations.impl.duckdb.sql_client import DuckDbSqlClient
from dlt.destinations.impl.duckdb.configuration import DuckDbClientConfiguration
from dlt.destinations.type_mapping import TypeMapper


HINT_TO_POSTGRES_ATTR: Dict[TColumnHint, str] = {"unique": "UNIQUE"}


class DuckDbTypeMapper(TypeMapper):
    sct_to_unbound_dbt = {
        "complex": "JSON",
        "text": "VARCHAR",
        "double": "DOUBLE",
        "bool": "BOOLEAN",
        "date": "DATE",
        # Duck does not allow specifying precision on timestamp with tz
        "timestamp": "TIMESTAMP WITH TIME ZONE",
        "bigint": "BIGINT",
        "binary": "BLOB",
        "time": "TIME",
    }

    sct_to_dbt = {
        # VARCHAR(n) is alias for VARCHAR in duckdb
        # "text": "VARCHAR(%i)",
        "decimal": "DECIMAL(%i,%i)",
        "wei": "DECIMAL(%i,%i)",
    }

    dbt_to_sct = {
        "VARCHAR": "text",
        "JSON": "complex",
        "DOUBLE": "double",
        "BOOLEAN": "bool",
        "DATE": "date",
        "TIMESTAMP WITH TIME ZONE": "timestamp",
        "BLOB": "binary",
        "DECIMAL": "decimal",
        "TIME": "time",
        # Int types
        "TINYINT": "bigint",
        "SMALLINT": "bigint",
        "INTEGER": "bigint",
        "BIGINT": "bigint",
        "HUGEINT": "bigint",
        "TIMESTAMP_S": "timestamp",
        "TIMESTAMP_MS": "timestamp",
        "TIMESTAMP_NS": "timestamp",
    }

    def to_db_integer_type(self, column: TColumnSchema = None, table: TTableSchema = None) -> str:
        precision = column.get("precision")
        if precision is None:
            return "BIGINT"
        # Precision is number of bits
        if precision <= 8:
            return "TINYINT"
        elif precision <= 16:
            return "SMALLINT"
        elif precision <= 32:
            return "INTEGER"
        elif precision <= 64:
            return "BIGINT"
        elif precision <= 128:
            return "HUGEINT"
        raise TerminalValueError(
            f"bigint with {precision} bits precision cannot be mapped into duckdb integer type"
        )

    def to_db_datetime_type(
        self,
        column: TColumnSchema = None,
        table: TTableSchema = None,
    ) -> str:
        column_name = column.get("name")
        table_name = table.get("name")
        timezone = column.get("timezone")
        precision = column.get("precision")

        if timezone and precision is not None:
            raise TerminalValueError(
                f"DuckDB does not support both timezone and precision for column '{column_name}' in"
                f" table '{table_name}'. To resolve this issue, either set timezone to False or"
                " None, or use the default precision."
            )

        if timezone:
            return "TIMESTAMP WITH TIME ZONE"

        # map precision to the appropriate TIMESTAMP type
        precision_map = {
            None: super().to_db_datetime_type(column, table),
            6: super().to_db_datetime_type(column, table),
            0: "TIMESTAMP_S",
            3: "TIMESTAMP_MS",
            9: "TIMESTAMP_NS",
        }
        if precision in precision_map:
            return precision_map[precision]

        raise TerminalValueError(
            f"Precision '{precision}' decimals after seconds cannot be mapped to a DuckDB TIMESTAMP"
            " type."
        )

    def from_db_type(
        self, db_type: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        # duckdb provides the types with scale and precision
        db_type = db_type.split("(")[0].upper()
        if db_type == "DECIMAL":
            if precision == 38 and scale == 0:
                return dict(data_type="wei", precision=precision, scale=scale)
        return super().from_db_type(db_type, precision, scale)


class DuckDbCopyJob(RunnableLoadJob, HasFollowupJobs):
    def __init__(self, file_path: str) -> None:
        super().__init__(file_path)
        self._job_client: "DuckDbClient" = None

    def run(self) -> None:
        self._sql_client = self._job_client.sql_client

        qualified_table_name = self._sql_client.make_qualified_table_name(self.load_table_name)
        if self._file_path.endswith("parquet"):
            source_format = "read_parquet"
            options = ", union_by_name=true"
        elif self._file_path.endswith("jsonl"):
            # NOTE: loading JSON does not work in practice on duckdb: the missing keys fail the load instead of being interpreted as NULL
            source_format = "read_json"  # newline delimited, compression auto
            options = ", COMPRESSION=GZIP" if FileStorage.is_gzipped(self._file_path) else ""
        else:
            raise ValueError(self._file_path)

        with self._sql_client.begin_transaction():
            self._sql_client.execute_sql(
                f"INSERT INTO {qualified_table_name} BY NAME SELECT * FROM"
                f" {source_format}('{self._file_path}' {options});"
            )


class DuckDbClient(InsertValuesJobClient):
    def __init__(
        self,
        schema: Schema,
        config: DuckDbClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        sql_client = DuckDbSqlClient(
            config.normalize_dataset_name(schema),
            config.normalize_staging_dataset_name(schema),
            config.credentials,
            capabilities,
        )
        super().__init__(schema, config, sql_client)
        self.config: DuckDbClientConfiguration = config
        self.sql_client: DuckDbSqlClient = sql_client  # type: ignore
        self.active_hints = HINT_TO_POSTGRES_ATTR if self.config.create_indexes else {}
        self.type_mapper = DuckDbTypeMapper(self.capabilities)

    def create_load_job(
        self, table: TTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        job = super().create_load_job(table, file_path, load_id, restore)
        if not job:
            job = DuckDbCopyJob(file_path)
        return job

    def _get_column_def_sql(self, c: TColumnSchema, table: TTableSchema = None) -> str:
        hints_str = " ".join(
            self.active_hints.get(h, "")
            for h in self.active_hints.keys()
            if c.get(h, False) is True
        )
        column_name = self.sql_client.escape_column_name(c["name"])
        return (
            f"{column_name} {self.type_mapper.to_db_type(c,table)} {hints_str} {self._gen_not_null(c.get('nullable', True))}"
        )

    def _from_db_type(
        self, pq_t: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        return self.type_mapper.from_db_type(pq_t, precision, scale)
