from typing import ClassVar, Dict, Optional, Sequence, Tuple, List, Any
from urllib.parse import urlparse

from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import FollowupJob, NewLoadJob, TLoadJobState, LoadJob, CredentialsConfiguration
from dlt.common.configuration.specs import AwsCredentialsWithoutDefaults
from dlt.common.data_types import TDataType
from dlt.common.storages.file_storage import FileStorage
from dlt.common.schema import TColumnSchema, Schema, TTableSchemaColumns
from dlt.common.schema.typing import TTableSchema, TWriteDisposition


from dlt.destinations.job_client_impl import SqlJobClientBase
from dlt.destinations.job_impl import EmptyLoadJob
from dlt.destinations.exceptions import LoadJobTerminalException

from dlt.destinations.snowflake import capabilities
from dlt.destinations.snowflake.configuration import SnowflakeClientConfiguration
from dlt.destinations.snowflake.sql_client import SnowflakeSqlClient
from dlt.destinations.sql_jobs import SqlStagingCopyJob
from dlt.destinations.snowflake.sql_client import SnowflakeSqlClient
from dlt.destinations.job_impl import NewReferenceJob
from dlt.destinations.sql_client import SqlClientBase

BIGINT_PRECISION = 19
MAX_NUMERIC_PRECISION = 38


SCT_TO_SNOW: Dict[TDataType, str] = {
    "complex": "VARIANT",
    "text": "VARCHAR",
    "double": "FLOAT",
    "bool": "BOOLEAN",
    "date": "DATE",
    "timestamp": "TIMESTAMP_TZ",
    "bigint": f"NUMBER({BIGINT_PRECISION},0)",  # Snowflake has no integer types
    "binary": "BINARY",
    "decimal": "NUMBER(%i,%i)",
}

SNOW_TO_SCT: Dict[str, TDataType] = {
    "VARCHAR": "text",
    "FLOAT": "double",
    "BOOLEAN": "bool",
    "DATE": "date",
    "TIMESTAMP_TZ": "timestamp",
    "BINARY": "binary",
    "VARIANT": "complex"
}


class SnowflakeLoadJob(LoadJob, FollowupJob):
    def __init__(
            self, file_path: str, table_name: str, load_id: str, client: SnowflakeSqlClient,
            stage_name: Optional[str] = None, keep_staged_files: bool = True, staging_credentials: Optional[CredentialsConfiguration] = None
    ) -> None:
        file_name = FileStorage.get_file_name_from_file_path(file_path)
        super().__init__(file_name)

        qualified_table_name = client.make_qualified_table_name(table_name)

        # extract and prepare some vars
        bucket_path = NewReferenceJob.resolve_reference(file_path) if NewReferenceJob.is_reference_job(file_path) else ""
        file_name = FileStorage.get_file_name_from_file_path(bucket_path) if bucket_path else file_name
        from_clause = ""
        credentials_clause = ""
        files_clause = ""
        stage_file_path = ""

        if bucket_path:
            # s3 credentials case
            if bucket_path.startswith("s3://") and staging_credentials and isinstance(staging_credentials, AwsCredentialsWithoutDefaults):
                credentials_clause = f"""CREDENTIALS=(AWS_KEY_ID='{staging_credentials.aws_access_key_id}' AWS_SECRET_KEY='{staging_credentials.aws_secret_access_key}')"""
                from_clause = f"FROM '{bucket_path}'"
            else:
                # ensure that gcs bucket path starts with gcs://, this is a requirement of snowflake
                bucket_path = bucket_path.replace("gs://", "gcs://")
                if not stage_name:
                    # when loading from bucket stage must be given
                    raise LoadJobTerminalException(file_path, f"Cannot load from bucket path {bucket_path} without a stage name. See https://dlthub.com/docs/dlt-ecosystem/destinations/snowflake for instructions on setting up the `stage_name`")
                from_clause = f"FROM @{stage_name}/"
                files_clause = f"FILES = ('{urlparse(bucket_path).path.lstrip('/')}')"
        else:
            # this means we have a local file
            if not stage_name:
                # Use implicit table stage by default: "SCHEMA_NAME"."%TABLE_NAME"
                stage_name = client.make_qualified_table_name('%'+table_name)
            stage_file_path = f'@{stage_name}/"{load_id}"/{file_name}'
            from_clause = f"FROM {stage_file_path}"

        # decide on source format, stage_file_path will either be a local file or a bucket path
        source_format = "( TYPE = 'JSON', BINARY_FORMAT = 'BASE64' )"
        if file_name.endswith("parquet"):
            source_format = "(TYPE = 'PARQUET', BINARY_AS_TEXT = FALSE)"

        with client.begin_transaction():
            # PUT and COPY in one tx if local file, otherwise only copy
            if not bucket_path:
                client.execute_sql(f'PUT file://{file_path} @{stage_name}/"{load_id}" OVERWRITE = TRUE, AUTO_COMPRESS = FALSE')
            client.execute_sql(
                f"""COPY INTO {qualified_table_name}
                {from_clause}
                {files_clause}
                {credentials_clause}
                FILE_FORMAT = {source_format}
                MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE'
                """
            )
            if stage_file_path and not keep_staged_files:
                client.execute_sql(f'REMOVE {stage_file_path}')


    def state(self) -> TLoadJobState:
        return "completed"

    def exception(self) -> str:
        raise NotImplementedError()

class SnowflakeStagingCopyJob(SqlStagingCopyJob):

    @classmethod
    def generate_sql(cls, table_chain: Sequence[TTableSchema], sql_client: SqlClientBase[Any]) -> List[str]:
        sql: List[str] = []
        for table in table_chain:
            with sql_client.with_staging_dataset(staging=True):
                staging_table_name = sql_client.make_qualified_table_name(table["name"])
            table_name = sql_client.make_qualified_table_name(table["name"])
            # drop destination table
            sql.append(f"DROP TABLE IF EXISTS {table_name};")
            # recreate destination table with data cloned from staging table
            sql.append(f"CREATE TABLE {table_name} CLONE {staging_table_name};")
        return sql


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
                file_path,
                table['name'],
                load_id,
                self.sql_client,
                stage_name=self.config.stage_name,
                keep_staged_files=self.config.keep_staged_files,
                staging_credentials=self.config.staging_credentials
            )
        return job

    def restore_file_load(self, file_path: str) -> LoadJob:
        return EmptyLoadJob.from_file_path(file_path, "completed")

    def _make_add_column_sql(self, new_columns: Sequence[TColumnSchema]) -> List[str]:
        # Override because snowflake requires multiple columns in a single ADD COLUMN clause
        return ["ADD COLUMN\n" + ",\n".join(self._get_column_def_sql(c) for c in new_columns)]

    def _create_optimized_replace_job(self, table_chain: Sequence[TTableSchema]) -> NewLoadJob:
        return SnowflakeStagingCopyJob.from_table_chain(table_chain, self.sql_client)

    def _get_table_update_sql(self, table_name: str, new_columns: Sequence[TColumnSchema], generate_alter: bool, separate_alters: bool = False) -> List[str]:
        sql = super()._get_table_update_sql(table_name, new_columns, generate_alter)

        cluster_list = [self.capabilities.escape_identifier(c['name']) for c in new_columns if c.get('cluster')]

        if cluster_list:
            sql[0] = sql[0] + "\nCLUSTER BY (" + ",".join(cluster_list) + ")"

        return sql

    @classmethod
    def _to_db_type(cls, sc_t: TDataType) -> str:
        if sc_t == "wei":
            return SCT_TO_SNOW["decimal"] % cls.capabilities.wei_precision
        if sc_t == "decimal":
            return SCT_TO_SNOW["decimal"] % cls.capabilities.decimal_precision
        return SCT_TO_SNOW[sc_t]

    @classmethod
    def _from_db_type(cls, bq_t: str, precision: Optional[int], scale: Optional[int]) -> TDataType:
        if bq_t == "NUMBER":
            if precision == BIGINT_PRECISION and scale == 0:
                return 'bigint'
            elif (precision, scale) == cls.capabilities.wei_precision:
                return 'wei'
            return 'decimal'
        return SNOW_TO_SCT.get(bq_t, "text")

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
