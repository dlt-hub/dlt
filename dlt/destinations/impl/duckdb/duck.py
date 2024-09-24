from typing import Dict, Optional

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.exceptions import TerminalValueError
from dlt.common.schema import TColumnSchema, TColumnHint, Schema
from dlt.common.destination.reference import (
    PreparedTableSchema,
    RunnableLoadJob,
    HasFollowupJobs,
    LoadJob,
)
from dlt.common.schema.typing import TColumnType, TTableFormat
from dlt.common.storages.file_storage import FileStorage

from dlt.destinations.insert_job_client import InsertValuesJobClient

from dlt.destinations.impl.duckdb.sql_client import DuckDbSqlClient
from dlt.destinations.impl.duckdb.configuration import DuckDbClientConfiguration


HINT_TO_POSTGRES_ATTR: Dict[TColumnHint, str] = {"unique": "UNIQUE"}


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
        self.type_mapper = self.capabilities.get_type_mapper()

    def create_load_job(
        self, table: PreparedTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        job = super().create_load_job(table, file_path, load_id, restore)
        if not job:
            job = DuckDbCopyJob(file_path)
        return job

    def _get_column_def_sql(self, c: TColumnSchema, table: PreparedTableSchema = None) -> str:
        hints_str = " ".join(
            self.active_hints.get(h, "")
            for h in self.active_hints.keys()
            if c.get(h, False) is True
        )
        column_name = self.sql_client.escape_column_name(c["name"])
        return (
            f"{column_name} {self.type_mapper.to_destination_type(c,table)} {hints_str} {self._gen_not_null(c.get('nullable', True))}"
        )

    def _from_db_type(
        self, pq_t: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        return self.type_mapper.from_destination_type(pq_t, precision, scale)
