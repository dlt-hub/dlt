from typing import ClassVar, Optional, Sequence, Tuple, List, Any
from urllib.parse import urlparse

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import (
    HasFollowupJobs,
    TLoadJobState,
    RunnableLoadJob,
    SupportsStagingDestination,
    FollowupJobRequest,
    LoadJob,
)
from dlt.common.schema import TColumnSchema, Schema, TTableSchemaColumns
from dlt.common.schema.typing import TTableSchema, TColumnType, TTableFormat, TColumnSchemaBase
from dlt.common.storages.file_storage import FileStorage
from dlt.common.utils import uniq_id
from dlt.destinations.exceptions import LoadJobTerminalException
from dlt.destinations.impl.dremio.configuration import DremioClientConfiguration
from dlt.destinations.impl.dremio.sql_client import DremioSqlClient
from dlt.destinations.job_client_impl import SqlJobClientWithStaging
from dlt.destinations.job_impl import FinalizedLoadJobWithFollowupJobs
from dlt.destinations.job_impl import ReferenceFollowupJobRequest
from dlt.destinations.sql_jobs import SqlMergeFollowupJob
from dlt.destinations.type_mapping import TypeMapper
from dlt.destinations.sql_client import SqlClientBase


class DremioTypeMapper(TypeMapper):
    BIGINT_PRECISION = 19
    sct_to_unbound_dbt = {
        "complex": "VARCHAR",
        "text": "VARCHAR",
        "double": "DOUBLE",
        "bool": "BOOLEAN",
        "date": "DATE",
        "timestamp": "TIMESTAMP",
        "bigint": "BIGINT",
        "binary": "VARBINARY",
        "time": "TIME",
    }

    sct_to_dbt = {
        "decimal": "DECIMAL(%i,%i)",
        "wei": "DECIMAL(%i,%i)",
    }

    dbt_to_sct = {
        "VARCHAR": "text",
        "DOUBLE": "double",
        "FLOAT": "double",
        "BOOLEAN": "bool",
        "DATE": "date",
        "TIMESTAMP": "timestamp",
        "VARBINARY": "binary",
        "BINARY": "binary",
        "BINARY VARYING": "binary",
        "VARIANT": "complex",
        "TIME": "time",
        "BIGINT": "bigint",
        "DECIMAL": "decimal",
    }

    def from_db_type(
        self, db_type: str, precision: Optional[int] = None, scale: Optional[int] = None
    ) -> TColumnType:
        if db_type == "DECIMAL":
            if (precision, scale) == self.capabilities.wei_precision:
                return dict(data_type="wei")
            return dict(data_type="decimal", precision=precision, scale=scale)
        return super().from_db_type(db_type, precision, scale)


class DremioMergeJob(SqlMergeFollowupJob):
    @classmethod
    def _new_temp_table_name(cls, name_prefix: str, sql_client: SqlClientBase[Any]) -> str:
        return sql_client.make_qualified_table_name(f"_temp_{name_prefix}_{uniq_id()}")

    @classmethod
    def _to_temp_table(cls, select_sql: str, temp_table_name: str) -> str:
        return f"CREATE TABLE {temp_table_name} AS {select_sql};"

    @classmethod
    def default_order_by(cls) -> str:
        return "NULL"


class DremioLoadJob(RunnableLoadJob, HasFollowupJobs):
    def __init__(
        self,
        file_path: str,
        stage_name: Optional[str] = None,
    ) -> None:
        super().__init__(file_path)
        self._stage_name = stage_name
        self._job_client: "DremioClient" = None

    def run(self) -> None:
        self._sql_client = self._job_client.sql_client

        qualified_table_name = self._sql_client.make_qualified_table_name(self.load_table_name)

        # extract and prepare some vars
        bucket_path = (
            ReferenceFollowupJobRequest.resolve_reference(self._file_path)
            if ReferenceFollowupJobRequest.is_reference_job(self._file_path)
            else ""
        )

        if not bucket_path:
            raise RuntimeError("Could not resolve bucket path.")

        file_name = (
            FileStorage.get_file_name_from_file_path(bucket_path)
            if bucket_path
            else self._file_name
        )

        bucket_url = urlparse(bucket_path)
        bucket_scheme = bucket_url.scheme
        if bucket_scheme == "s3" and self._stage_name:
            from_clause = (
                f"FROM '@{self._stage_name}/{bucket_url.hostname}/{bucket_url.path.lstrip('/')}'"
            )
        else:
            raise LoadJobTerminalException(
                self._file_path, "Only s3 staging currently supported in Dremio destination"
            )

        source_format = file_name.split(".")[-1]

        self._sql_client.execute_sql(f"""COPY INTO {qualified_table_name}
            {from_clause}
            FILE_FORMAT '{source_format}'
            """)


class DremioClient(SqlJobClientWithStaging, SupportsStagingDestination):
    def __init__(
        self,
        schema: Schema,
        config: DremioClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        sql_client = DremioSqlClient(
            config.normalize_dataset_name(schema),
            config.normalize_staging_dataset_name(schema),
            config.credentials,
            capabilities,
        )
        super().__init__(schema, config, sql_client)
        self.config: DremioClientConfiguration = config
        self.sql_client: DremioSqlClient = sql_client  # type: ignore
        self.type_mapper = DremioTypeMapper(self.capabilities)

    def create_load_job(
        self, table: TTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        job = super().create_load_job(table, file_path, load_id, restore)

        if not job:
            job = DremioLoadJob(
                file_path=file_path,
                stage_name=self.config.staging_data_source,
            )
        return job

    def _get_table_update_sql(
        self,
        table_name: str,
        new_columns: Sequence[TColumnSchema],
        generate_alter: bool,
        separate_alters: bool = False,
    ) -> List[str]:
        sql = super()._get_table_update_sql(table_name, new_columns, generate_alter)

        if not generate_alter:
            partition_list = [
                self.sql_client.escape_column_name(c["name"])
                for c in new_columns
                if c.get("partition")
            ]
            if partition_list:
                sql[0] += "\nPARTITION BY (" + ",".join(partition_list) + ")"

            sort_list = [
                self.sql_client.escape_column_name(c["name"]) for c in new_columns if c.get("sort")
            ]
            if sort_list:
                sql[0] += "\nLOCALSORT BY (" + ",".join(sort_list) + ")"

        return sql

    def _from_db_type(
        self, bq_t: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        return self.type_mapper.from_db_type(bq_t, precision, scale)

    def _get_column_def_sql(self, c: TColumnSchema, table_format: TTableFormat = None) -> str:
        name = self.sql_client.escape_column_name(c["name"])
        return (
            f"{name} {self.type_mapper.to_db_type(c)} {self._gen_not_null(c.get('nullable', True))}"
        )

    def _create_merge_followup_jobs(
        self, table_chain: Sequence[TTableSchema]
    ) -> List[FollowupJobRequest]:
        return [DremioMergeJob.from_table_chain(table_chain, self.sql_client)]

    def _make_add_column_sql(
        self, new_columns: Sequence[TColumnSchema], table_format: TTableFormat = None
    ) -> List[str]:
        return ["ADD COLUMNS (" + ", ".join(self._get_column_def_sql(c) for c in new_columns) + ")"]

    def should_truncate_table_before_load_on_staging_destination(self, table: TTableSchema) -> bool:
        return self.config.truncate_tables_on_staging_destination_before_load
