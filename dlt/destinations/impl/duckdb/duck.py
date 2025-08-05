from copy import copy
from typing import Dict, Iterable, List, Optional, Sequence

from dlt.common import logger
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.schema import TColumnHint, Schema
from dlt.common.destination.client import (
    PreparedTableSchema,
    RunnableLoadJob,
    HasFollowupJobs,
    LoadJob,
)
from dlt.common.schema.typing import TColumnSchema, TColumnType, TTableFormat
from dlt.common.schema.utils import has_default_column_prop_value
from dlt.common.storages.file_storage import FileStorage
from dlt.common.storages.load_storage import ParsedLoadJobFileName

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

        parsed_file = ParsedLoadJobFileName.parse(self._file_path)
        if parsed_file.file_format == "parquet":
            source_format = "read_parquet"
            options = ", union_by_name=true"
        elif parsed_file.file_format in ["jsonl", "typed-jsonl"]:
            # NOTE: loading JSON does not work in practice on duckdb: the missing keys fail the load instead of being interpreted as NULL
            source_format = "read_json"  # newline delimited, compression auto
            options = ""
        else:
            raise ValueError(self._file_path)

        with self._sql_client.begin_transaction():
            self._sql_client.execute_sql(
                f"INSERT INTO {qualified_table_name} BY NAME SELECT * FROM"
                f" {source_format}('{self._file_path}' {options})"
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

    def initialize_storage(self, truncate_tables: Iterable[str] = None) -> None:
        self.sql_client.warn_if_catalog_equals_dataset_name()
        super().initialize_storage(truncate_tables)

    def _from_db_type(
        self, pq_t: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        return self.type_mapper.from_destination_type(pq_t, precision, scale)

    # def _make_add_column_sql(
    #     self, new_columns: Sequence[TColumnSchema], table: PreparedTableSchema = None
    # ) -> List[str]:
    #     # skip nullability on duckdb
    #     new_columns = list(new_columns)
    #     for idx, c in enumerate(new_columns):
    #         if not has_default_column_prop_value("nullable", c.get("nullable")):
    #             logger.warning(
    #                 f"Adding new NOT NULL column '{c['name']}' to existing table '{table['name']}'"
    #                 " on duckdb. NOT NULL will be ignored."
    #             )
    #             # make copy so original new column is not changed
    #             new_columns[idx] = copy(c)
    #             new_columns[idx]["nullable"] = True

    #     return [f"ADD COLUMN {self._get_column_def_sql(c, table)}" for c in new_columns]
