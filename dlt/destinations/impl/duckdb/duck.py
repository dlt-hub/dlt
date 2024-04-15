import threading
from typing import ClassVar, Dict, Optional

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.data_types import TDataType
from dlt.common.exceptions import TerminalValueError
from dlt.common.schema import TColumnSchema, TColumnHint, Schema
from dlt.common.destination.reference import LoadJob, FollowupJob, TLoadJobState
from dlt.common.schema.typing import TTableSchema, TColumnType, TTableFormat
from dlt.common.storages.file_storage import FileStorage
from dlt.common.utils import maybe_context

from dlt.destinations.insert_job_client import InsertValuesJobClient

from dlt.destinations.impl.duckdb import capabilities
from dlt.destinations.impl.duckdb.sql_client import DuckDbSqlClient
from dlt.destinations.impl.duckdb.configuration import DuckDbClientConfiguration
from dlt.destinations.type_mapping import TypeMapper


HINT_TO_POSTGRES_ATTR: Dict[TColumnHint, str] = {"unique": "UNIQUE"}

# duckdb cannot load PARQUET to the same table in parallel. so serialize it per table
PARQUET_TABLE_LOCK = threading.Lock()
TABLES_LOCKS: Dict[str, threading.Lock] = {}


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

    def to_db_integer_type(
        self, precision: Optional[int], table_format: TTableFormat = None
    ) -> str:
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
        self, precision: Optional[int], table_format: TTableFormat = None
    ) -> str:
        if precision is None or precision == 6:
            return super().to_db_datetime_type(precision, table_format)
        if precision == 0:
            return "TIMESTAMP_S"
        if precision == 3:
            return "TIMESTAMP_MS"
        if precision == 9:
            return "TIMESTAMP_NS"
        raise TerminalValueError(
            f"timestamp with {precision} decimals after seconds cannot be mapped into duckdb"
            " TIMESTAMP type"
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


class DuckDbCopyJob(LoadJob, FollowupJob):
    def __init__(self, table_name: str, file_path: str, sql_client: DuckDbSqlClient) -> None:
        super().__init__(FileStorage.get_file_name_from_file_path(file_path))

        qualified_table_name = sql_client.make_qualified_table_name(table_name)
        if file_path.endswith("parquet"):
            source_format = "PARQUET"
            options = ""
            # lock when creating a new lock
            with PARQUET_TABLE_LOCK:
                # create or get lock per table name
                lock: threading.Lock = TABLES_LOCKS.setdefault(
                    qualified_table_name, threading.Lock()
                )
        elif file_path.endswith("jsonl"):
            # NOTE: loading JSON does not work in practice on duckdb: the missing keys fail the load instead of being interpreted as NULL
            source_format = "JSON"  # newline delimited, compression auto
            options = ", COMPRESSION GZIP" if FileStorage.is_gzipped(file_path) else ""
            lock = None
        else:
            raise ValueError(file_path)

        with maybe_context(lock):
            with sql_client.begin_transaction():
                sql_client.execute_sql(
                    f"COPY {qualified_table_name} FROM '{file_path}' ( FORMAT"
                    f" {source_format} {options});"
                )

    def state(self) -> TLoadJobState:
        return "completed"

    def exception(self) -> str:
        raise NotImplementedError()


class DuckDbClient(InsertValuesJobClient):
    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, schema: Schema, config: DuckDbClientConfiguration) -> None:
        sql_client = DuckDbSqlClient(config.normalize_dataset_name(schema), config.credentials)
        super().__init__(schema, config, sql_client)
        self.config: DuckDbClientConfiguration = config
        self.sql_client: DuckDbSqlClient = sql_client  # type: ignore
        self.active_hints = HINT_TO_POSTGRES_ATTR if self.config.create_indexes else {}
        self.type_mapper = DuckDbTypeMapper(self.capabilities)

    def start_file_load(self, table: TTableSchema, file_path: str, load_id: str) -> LoadJob:
        job = super().start_file_load(table, file_path, load_id)
        if not job:
            job = DuckDbCopyJob(table["name"], file_path, self.sql_client)
        return job

    def _get_column_def_sql(self, c: TColumnSchema, table_format: TTableFormat = None) -> str:
        hints_str = " ".join(
            self.active_hints.get(h, "")
            for h in self.active_hints.keys()
            if c.get(h, False) is True
        )
        column_name = self.capabilities.escape_identifier(c["name"])
        return (
            f"{column_name} {self.type_mapper.to_db_type(c)} {hints_str} {self._gen_not_null(c.get('nullable', True))}"
        )

    def _from_db_type(
        self, pq_t: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        return self.type_mapper.from_db_type(pq_t, precision, scale)
