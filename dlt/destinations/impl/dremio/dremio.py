from typing import ClassVar, Optional, Sequence, Tuple, List, Any
from urllib.parse import urlparse

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.client import (
    HasFollowupJobs,
    PreparedTableSchema,
    TLoadJobState,
    RunnableLoadJob,
    SupportsStagingDestination,
    FollowupJobRequest,
    LoadJob,
)
from dlt.common.schema import TColumnSchema, Schema
from dlt.common.schema.typing import TColumnType, TTableFormat
from dlt.common.storages.file_storage import FileStorage
from dlt.common.utils import uniq_id
from dlt.destinations.exceptions import LoadJobTerminalException
from dlt.destinations.impl.dremio.configuration import DremioClientConfiguration
from dlt.destinations.impl.dremio.sql_client import DremioSqlClient
from dlt.destinations.job_client_impl import SqlJobClientWithStagingDataset
from dlt.destinations.job_impl import ReferenceFollowupJobRequest
from dlt.destinations.sql_jobs import SqlMergeFollowupJob
from dlt.destinations.sql_client import SqlClientBase
from dlt.destinations.utils import get_deterministic_temp_table_name


class DremioMergeJob(SqlMergeFollowupJob):
    @classmethod
    def _new_temp_table_name(cls, table_name: str, op: str, sql_client: SqlClientBase[Any]) -> str:
        with sql_client.with_staging_dataset():
            return sql_client.make_qualified_table_name(
                cls._shorten_table_name(
                    get_deterministic_temp_table_name(table_name, op), sql_client
                )
            )

    @classmethod
    def _to_temp_table(cls, select_sql: str, temp_table_name: str, unique_column: str) -> str:
        return (
            f"DROP TABLE IF EXISTS {temp_table_name}; CREATE TABLE {temp_table_name} AS"
            f" {select_sql}"
        )

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
            raise RuntimeError("Could not resolve `bucket_path`.")

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


class DremioClient(SqlJobClientWithStagingDataset, SupportsStagingDestination):
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
        self.type_mapper = self.capabilities.get_type_mapper()

    def create_load_job(
        self, table: PreparedTableSchema, file_path: str, load_id: str, restore: bool = False
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
        return self.type_mapper.from_destination_type(bq_t, precision, scale)

    def _create_merge_followup_jobs(
        self, table_chain: Sequence[PreparedTableSchema]
    ) -> List[FollowupJobRequest]:
        return [DremioMergeJob.from_table_chain(table_chain, self.sql_client)]

    def _make_add_column_sql(
        self, new_columns: Sequence[TColumnSchema], table: PreparedTableSchema = None
    ) -> List[str]:
        return [
            "ADD COLUMNS ("
            + ", ".join(self._get_column_def_sql(c, table) for c in new_columns)
            + ")"
        ]

    def should_truncate_table_before_load_on_staging_destination(self, table_name: str) -> bool:
        return self.config.truncate_tables_on_staging_destination_before_load
