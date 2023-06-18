from pathlib import Path
from typing import ClassVar, Dict, Optional, Sequence, Tuple, List, cast, Iterable

from dlt.common import json, logger
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.configuration.specs import GcpServiceAccountCredentialsWithoutDefaults
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import FollowupJob, NewLoadJob, TLoadJobState, LoadJob
from dlt.common.data_types import TDataType
from dlt.common.storages.file_storage import FileStorage
from dlt.common.schema import TColumnSchema, Schema, TTableSchemaColumns, TSchemaTables
from dlt.common.schema.typing import TTableSchema, TWriteDisposition
from dlt.common.wei import EVM_DECIMAL_PRECISION

from dlt.destinations.job_client_impl import SqlJobClientBase
from dlt.destinations.job_impl import EmptyLoadJob
from dlt.destinations.exceptions import DestinationSchemaWillNotUpdate, DestinationTransientException, LoadJobNotExistsException, LoadJobTerminalException, LoadJobUnknownTableException

from dlt.destinations.snowflake import capabilities
from dlt.destinations.snowflake.configuration import SnowflakeClientConfiguration
from dlt.destinations.snowflake.sql_client import SnowflakeSqlClient
from dlt.destinations.sql_merge_job import SqlMergeJob
from dlt.destinations.snowflake.sql_client import SnowflakeSqlClient

BIGINT_PRECISION = 19
MAX_NUMERIC_PRECISION = 38


SCT_TO_BQT: Dict[TDataType, str] = {
    "complex": "VARIANT",
    "text": "VARCHAR",
    "double": "FLOAT",
    "bool": "BOOLEAN",
    "date": "DATE",
    "timestamp": "TIMESTAMP_TZ",
    "bigint": f"NUMBER({BIGINT_PRECISION},0)",  # Snowflake has no integer types
    "binary": "BINARY",
    "decimal": f"NUMBER({DEFAULT_NUMERIC_PRECISION},{DEFAULT_NUMERIC_SCALE})",
}

BQT_TO_SCT: Dict[str, TDataType] = {
    "VARCHAR": "text",
    "FLOAT": "double",
    "BOOLEAN": "bool",
    "DATE": "date",
    "TIMESTAMP_TZ": "timestamp",
    "BINARY": "binary",
    "VARIANT": "complex"
}

class SnowflakeLoadJob(LoadJob, FollowupJob):
    def __init__(self, file_path: str, table_name: str, write_disposition: TWriteDisposition, load_id: str, client: SnowflakeSqlClient) -> None:
        file_name = FileStorage.get_file_name_from_file_path(file_path)
        super().__init__(file_name)

        with client.with_staging_dataset(write_disposition == "merge"):
            qualified_table_name = client.make_qualified_table_name(table_name)
            stage_name = f'"{table_name}_{load_id}"'
            # Create stage and file format
            client.execute_sql(
                """
                CREATE FILE FORMAT IF NOT EXISTS dlt_jsonl_format
                TYPE = 'JSON'
                BINARY_FORMAT = 'BASE64'
                """
            )
            client.execute_sql(f"CREATE STAGE IF NOT EXISTS {stage_name} FILE_FORMAT = dlt_jsonl_format")
            with client.begin_transaction():
                # PUT and copy files in one transaction
                client.execute_sql(f"PUT file://{file_path} @{stage_name} OVERWRITE = TRUE")
                if write_disposition == "replace":
                    client.execute_sql(f"TRUNCATE TABLE IF EXISTS {qualified_table_name}")
                client.execute_sql(
                    f"""COPY INTO {qualified_table_name}
                    FROM @{stage_name}
                    FILE_FORMAT = dlt_jsonl_format
                    MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE'
                    """
                )


    def state(self) -> TLoadJobState:
        return "completed"

    def exception(self) -> str:
        raise NotImplementedError()


class SnowflakeClient(SqlJobClientBase):

    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, schema: Schema, config: SnowflakeClientConfiguration) -> None:
        sql_client = SnowflakeSqlClient(
            self.make_dataset_name(schema, config.dataset_name, config.default_schema_name),
            config.credentials
        )
        super().__init__(schema, config, sql_client)
        self.config: SnowflakeClientConfiguration = config
        self.sql_client: SnowflakeSqlClient = sql_client  # type: ignore

    def start_file_load(self, table: TTableSchema, file_path: str, load_id: str) -> LoadJob:
        job = super().start_file_load(table, file_path, load_id)

        if not job:
            job = SnowflakeLoadJob(
                file_path, table['name'], table['write_disposition'], load_id, self.sql_client
            )
        return job

    def restore_file_load(self, file_path: str) -> LoadJob:
        return EmptyLoadJob.from_file_path(file_path, "completed")

    def _make_add_column_sql(self, new_columns: Sequence[TColumnSchema]) -> List[str]:
        # Override because snowflake requires multiple columns in a single ADD COLUMN clause
        return ["ADD COLUMN\n" + ",\n".join(self._get_column_def_sql(c) for c in new_columns)]

    def _execute_schema_update_sql(self, only_tables: Iterable[str]) -> TSchemaTables:
        sql_scripts, schema_update = self._build_schema_update_sql(only_tables)
        # Snowflake doesn't support multiple statements per call, so we run one at a time
        for statement in sql_scripts:
            self.sql_client.execute_sql(statement)
        self._update_schema_in_storage(self.schema)
        return schema_update

    def _get_table_update_sql(self, table_name: str, new_columns: Sequence[TColumnSchema], generate_alter: bool, separate_alters: bool = False) -> List[str]:
        sql = super()._get_table_update_sql(table_name, new_columns, generate_alter)

        cluster_list = [self.capabilities.escape_identifier(c['name']) for c in new_columns if c.get('cluster')]

        if cluster_list:
            sql[0] = sql[0] + "\nCLUSTER BY " + ",".join(cluster_list)

        return sql

    @staticmethod
    def _to_db_type(sc_t: TDataType) -> str:
        if sc_t == 'wei':
            return "NUMBER(38,0)"
        return SCT_TO_BQT[sc_t]

    @staticmethod
    def _from_db_type(bq_t: str, precision: Optional[int], scale: Optional[int]) -> TDataType:
        if bq_t == "NUMBER":
            if precision == BIGINT_PRECISION and scale == 0:
                return 'bigint'
            elif precision == MAX_NUMERIC_PRECISION and scale == 0:
                return 'wei'
            return 'decimal'
        return BQT_TO_SCT.get(bq_t, "text")

    def _get_column_def_sql(self, c: TColumnSchema) -> str:
        name = self.capabilities.escape_identifier(c["name"])
        return f"{name} {self._to_db_type(c['data_type'])} {self._gen_not_null(c['nullable'])}"

    def get_storage_table(self, table_name: str) -> Tuple[bool, TTableSchemaColumns]:
        table_name = table_name.upper()  # All snowflake tables are uppercased in information schema
        exists, table = super().get_storage_table(table_name)
        if not exists:
            return exists, table
        # Snowflake converts all unquoted columns to UPPER CASE
        # Convert back to lower case to enable comparison with dlt schema
        table = {col_name.lower(): dict(col, name=col_name.lower()) for col_name, col in table.items()}  # type: ignore
        return exists, table
