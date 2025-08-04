from typing import Optional, Sequence, List, Dict

from dlt.common import logger
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.client import (
    FollowupJobRequest,
    HasFollowupJobs,
    LoadJob,
    PreparedTableSchema,
    RunnableLoadJob,
    CredentialsConfiguration,
    SupportsStagingDestination,
)
from dlt.common.schema.utils import get_columns_names_with_prop
from dlt.common.storages.file_storage import FileStorage
from dlt.common.schema import TColumnSchema, Schema, TColumnHint
from dlt.common.schema.typing import TColumnType, TTableSchema

from dlt.common.utils import uniq_id
from dlt.destinations.impl.snowflake.utils import gen_copy_sql
from dlt.destinations.job_client_impl import SqlJobClientWithStagingDataset

from dlt.destinations.impl.snowflake.configuration import SnowflakeClientConfiguration
from dlt.destinations.impl.snowflake.sql_client import SnowflakeSqlClient
from dlt.destinations.job_impl import ReferenceFollowupJobRequest
from dlt.destinations.sql_jobs import SqlMergeFollowupJob
from dlt.destinations.path_utils import get_file_format_and_compression

SUPPORTED_HINTS: Dict[TColumnHint, str] = {"unique": "UNIQUE"}


class SnowflakeMergeJob(SqlMergeFollowupJob):
    @classmethod
    def gen_key_table_clauses(
        cls,
        root_table_name: str,
        staging_root_table_name: str,
        key_clauses: Sequence[str],
        for_delete: bool,
    ) -> List[str]:
        sql: List[str] = [
            f"FROM {root_table_name} AS d WHERE EXISTS (SELECT 1 FROM {staging_root_table_name} AS"
            f" s WHERE {clause.format(d='d', s='s')})"
            for clause in key_clauses
        ]
        return sql


class SnowflakeLoadJob(RunnableLoadJob, HasFollowupJobs):
    def __init__(
        self,
        file_path: str,
        config: SnowflakeClientConfiguration,
        stage_name: Optional[str] = None,
        keep_staged_files: bool = True,
        staging_credentials: Optional[CredentialsConfiguration] = None,
    ) -> None:
        super().__init__(file_path)
        self._keep_staged_files = keep_staged_files
        self._staging_credentials = staging_credentials
        self._config = config
        self._stage_name = stage_name
        self._job_client: "SnowflakeClient" = None

    def run(self) -> None:
        self._sql_client = self._job_client.sql_client

        # resolve reference
        is_local_file = not ReferenceFollowupJobRequest.is_reference_job(self._file_path)
        file_url = (
            self._file_path
            if is_local_file
            else ReferenceFollowupJobRequest.resolve_reference(self._file_path)
        )
        # take file name
        file_name = FileStorage.get_file_name_from_file_path(file_url)
        file_format, _ = get_file_format_and_compression(file_name)

        qualified_table_name = self._sql_client.make_qualified_table_name(self.load_table_name)
        # this means we have a local file
        stage_file_path: str = ""
        if is_local_file:
            if not self._stage_name:
                # Use implicit table stage by default: "SCHEMA_NAME"."%TABLE_NAME"
                self._stage_name = self._sql_client.make_qualified_table_name(
                    "%" + self.load_table_name
                )
            stage_file_path = f'@{self._stage_name}/"{self._load_id}"/{file_name}'

        stage_bucket_url = None
        if self._config.staging_config and self._config.staging_config.bucket_url:
            stage_bucket_url = self._config.staging_config.bucket_url

        copy_sql = gen_copy_sql(
            file_url=file_url,
            qualified_table_name=qualified_table_name,
            loader_file_format=file_format,  # type: ignore[arg-type]
            is_case_sensitive=self._sql_client.capabilities.generates_case_sensitive_identifiers(),
            stage_name=self._stage_name,
            stage_bucket_url=stage_bucket_url,
            local_stage_file_path=stage_file_path,
            staging_credentials=self._staging_credentials,
            csv_format=self._config.csv_format,
            use_vectorized_scanner=self._config.use_vectorized_scanner,
        )

        with self._sql_client.begin_transaction():
            # PUT and COPY in one tx if local file, otherwise only copy
            if is_local_file:
                self._sql_client.execute_sql(
                    f'PUT file://{self._file_path} @{self._stage_name}/"{self._load_id}" OVERWRITE'
                    " = TRUE, AUTO_COMPRESS = FALSE"
                )
            self._sql_client.execute_sql(copy_sql)
            if stage_file_path and not self._keep_staged_files:
                self._sql_client.execute_sql(f"REMOVE {stage_file_path}")


class SnowflakeClient(SqlJobClientWithStagingDataset, SupportsStagingDestination):
    def __init__(
        self,
        schema: Schema,
        config: SnowflakeClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        sql_client = SnowflakeSqlClient(
            config.normalize_dataset_name(schema),
            config.normalize_staging_dataset_name(schema),
            config.credentials,
            capabilities,
            config.query_tag,
        )
        super().__init__(schema, config, sql_client)
        self.config: SnowflakeClientConfiguration = config
        self.sql_client: SnowflakeSqlClient = sql_client  # type: ignore
        self.type_mapper = self.capabilities.get_type_mapper()
        self.active_hints = SUPPORTED_HINTS if self.config.create_indexes else {}

    def create_load_job(
        self, table: PreparedTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        job = super().create_load_job(table, file_path, load_id, restore)

        if not job:
            job = SnowflakeLoadJob(
                file_path,
                self.config,
                stage_name=self.config.stage_name,
                keep_staged_files=self.config.keep_staged_files,
                staging_credentials=(
                    self.config.staging_config.credentials if self.config.staging_config else None
                ),
            )
        return job

    def _create_merge_followup_jobs(
        self, table_chain: Sequence[PreparedTableSchema]
    ) -> List[FollowupJobRequest]:
        return [SnowflakeMergeJob.from_table_chain(table_chain, self.sql_client)]

    def _make_add_column_sql(
        self, new_columns: Sequence[TColumnSchema], table: PreparedTableSchema = None
    ) -> List[str]:
        # Override because snowflake requires multiple columns in a single ADD COLUMN clause
        return [
            "ADD COLUMN\n" + ",\n".join(self._get_column_def_sql(c, table) for c in new_columns)
        ]

    def _get_constraints_sql(
        self, table_name: str, new_columns: Sequence[TColumnSchema], generate_alter: bool
    ) -> str:
        # "primary_key": "PRIMARY KEY"
        if self.config.create_indexes:
            partial: TTableSchema = {
                "name": table_name,
                "columns": {c["name"]: c for c in new_columns},
            }
            # Add PK constraint if pk_columns exist
            pk_columns = get_columns_names_with_prop(partial, "primary_key")
            if pk_columns:
                if generate_alter:
                    logger.warning(
                        f"PRIMARY KEY on {table_name} constraint cannot be added in ALTER TABLE and"
                        " is ignored"
                    )
                else:
                    pk_constraint_name = list(
                        self._norm_and_escape_columns(f"PK_{table_name}_{uniq_id(4)}")
                    )[0]
                    quoted_pk_cols = ", ".join(
                        self.sql_client.escape_column_name(col) for col in pk_columns
                    )
                    return f",\nCONSTRAINT {pk_constraint_name} PRIMARY KEY ({quoted_pk_cols})"
        return ""

    def _get_table_update_sql(
        self,
        table_name: str,
        new_columns: Sequence[TColumnSchema],
        generate_alter: bool,
        separate_alters: bool = False,
    ) -> List[str]:
        sql = super()._get_table_update_sql(table_name, new_columns, generate_alter)

        cluster_list = [
            self.sql_client.escape_column_name(c["name"]) for c in new_columns if c.get("cluster")
        ]

        if cluster_list:
            sql[0] = sql[0] + "\nCLUSTER BY (" + ",".join(cluster_list) + ")"

        return sql

    def _from_db_type(
        self, bq_t: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        return self.type_mapper.from_destination_type(bq_t, precision, scale)

    def should_truncate_table_before_load_on_staging_destination(self, table_name: str) -> bool:
        return self.config.truncate_tables_on_staging_destination_before_load
