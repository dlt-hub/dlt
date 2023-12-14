from typing import ClassVar, Dict, Optional, Sequence, Tuple, List, Any, Iterable, Type
from urllib.parse import urlparse, urlunparse

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import FollowupJob, NewLoadJob, TLoadJobState, LoadJob, CredentialsConfiguration
from dlt.common.configuration.specs import AwsCredentialsWithoutDefaults, AzureCredentials, AzureCredentialsWithoutDefaults
from dlt.common.data_types import TDataType
from dlt.common.storages.file_storage import FileStorage
from dlt.common.schema import TColumnSchema, Schema, TTableSchemaColumns
from dlt.common.schema.typing import TTableSchema, TColumnType, TSchemaTables


from dlt.destinations.job_client_impl import SqlJobClientWithStaging
from dlt.destinations.job_impl import EmptyLoadJob
from dlt.destinations.exceptions import LoadJobTerminalException

from dlt.destinations.databricks import capabilities
from dlt.destinations.databricks.configuration import DatabricksClientConfiguration
from dlt.destinations.databricks.sql_client import DatabricksSqlClient
from dlt.destinations.sql_jobs import SqlStagingCopyJob
from dlt.destinations.job_impl import NewReferenceJob
from dlt.destinations.sql_client import SqlClientBase
from dlt.destinations.type_mapping import TypeMapper


class DatabricksTypeMapper(TypeMapper):
    sct_to_unbound_dbt = {
        "complex": "ARRAY",  # Databricks supports complex types like ARRAY
        "text": "STRING",
        "double": "DOUBLE",
        "bool": "BOOLEAN",
        "date": "DATE",
        "timestamp": "TIMESTAMP",  # TIMESTAMP for local timezone
        "bigint": "BIGINT",
        "binary": "BINARY",
        "decimal": "DECIMAL",  # DECIMAL(p,s) format
        "float": "FLOAT",
        "int": "INT",
        "smallint": "SMALLINT",
        "tinyint": "TINYINT",
        "void": "VOID",
        "interval": "INTERVAL",
        "map": "MAP",
        "struct": "STRUCT",
    }

    dbt_to_sct = {
        "STRING": "text",
        "DOUBLE": "double",
        "BOOLEAN": "bool",
        "DATE": "date",
        "TIMESTAMP": "timestamp",
        "BIGINT": "bigint",
        "BINARY": "binary",
        "DECIMAL": "decimal",
        "FLOAT": "float",
        "INT": "int",
        "SMALLINT": "smallint",
        "TINYINT": "tinyint",
        "VOID": "void",
        "INTERVAL": "interval",
        "MAP": "map",
        "STRUCT": "struct",
        "ARRAY": "complex"
    }

    sct_to_dbt = {
        "text": "STRING",
        "double": "DOUBLE",
        "bool": "BOOLEAN",
        "date": "DATE",
        "timestamp": "TIMESTAMP",
        "bigint": "BIGINT",
        "binary": "BINARY",
        "decimal": "DECIMAL(%i,%i)",
        "float": "FLOAT",
        "int": "INT",
        "smallint": "SMALLINT",
        "tinyint": "TINYINT",
        "void": "VOID",
        "interval": "INTERVAL",
        "map": "MAP",
        "struct": "STRUCT",
        "complex": "ARRAY"
    }


    def from_db_type(self, db_type: str, precision: Optional[int] = None, scale: Optional[int] = None) -> TColumnType:
        if db_type == "NUMBER":
            if precision == self.BIGINT_PRECISION and scale == 0:
                return dict(data_type='bigint')
            elif (precision, scale) == self.capabilities.wei_precision:
                return dict(data_type='wei')
            return dict(data_type='decimal', precision=precision, scale=scale)
        return super().from_db_type(db_type, precision, scale)


class DatabricksLoadJob(LoadJob, FollowupJob):
    def __init__(
        self,
        file_path: str,
        table_name: str,
        load_id: str,
        client: DatabricksSqlClient,
        stage_name: Optional[str] = None,
        keep_staged_files: bool = True,
        staging_credentials: Optional[CredentialsConfiguration] = None
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
        format_options = ""
        copy_options = "COPY_OPTIONS ('mergeSchema'='true')"

        if bucket_path:
            bucket_url = urlparse(bucket_path)
            bucket_scheme = bucket_url.scheme
            # referencing an external s3/azure stage does not require explicit credentials
            if bucket_scheme in ["s3", "az"] and stage_name:
                from_clause = f"FROM ('{bucket_path}')"
            # referencing an staged files via a bucket URL requires explicit AWS credentials
            if bucket_scheme == "s3" and staging_credentials and isinstance(staging_credentials, AwsCredentialsWithoutDefaults):
                credentials_clause = f"""WITH(CREDENTIAL(AWS_KEY_ID='{staging_credentials.aws_access_key_id}', AWS_SECRET_KEY='{staging_credentials.aws_secret_access_key}'))"""
                from_clause = f"FROM '{bucket_path}'"
            elif bucket_scheme == "az" and staging_credentials and isinstance(staging_credentials, AzureCredentialsWithoutDefaults):
                # Explicit azure credentials are needed to load from bucket without a named stage
                credentials_clause = f"""WITH(CREDENTIAL(AZURE_SAS_TOKEN='{staging_credentials.azure_storage_sas_token}'))"""
                # Converts an az://<container_name>/<path> to abfss://<container_name>@<storage_account_name>.dfs.core.windows.net/<path>
                # as required by snowflake
                _path = bucket_url.path
                bucket_path = urlunparse(
                    bucket_url._replace(
                        scheme="abfss",
                        netloc=f"{bucket_url.netloc}@{staging_credentials.azure_storage_account_name}.dfs.core.windows.net",
                        path=_path
                    )
                )
                from_clause = f"FROM '{bucket_path}'"
            else:
                # ensure that gcs bucket path starts with gs://, this is a requirement of databricks
                bucket_path = bucket_path.replace("gs://", "gcs://")
                if not stage_name:
                    # when loading from bucket stage must be given
                    raise LoadJobTerminalException(file_path, f"Cannot load from bucket path {bucket_path} without a stage name. See https://dlthub.com/docs/dlt-ecosystem/destinations/databricks for instructions on setting up the `stage_name`")
                from_clause = f"FROM ('{bucket_path}')"
        # else:
        #     # this means we have a local file
        #     if not stage_name:
        #         # Use implicit table stage by default: "SCHEMA_NAME"."%TABLE_NAME"
        #         stage_name = client.make_qualified_table_name('%'+table_name)
        #     stage_file_path = f'@{stage_name}/"{load_id}"/{file_name}'
        #     from_clause = f"FROM {stage_file_path}"

        # decide on source format, stage_file_path will either be a local file or a bucket path
        source_format = "JSON"
        if file_name.endswith("parquet"):
            source_format = "PARQUET"

        with client.begin_transaction():
            client.execute_sql(
                f"""COPY INTO {qualified_table_name}
                {from_clause}
                {files_clause}
                {credentials_clause}
                FILEFORMAT = {source_format}
                {format_options}
                {copy_options}
                """
            )
            # if stage_file_path and not keep_staged_files:
            #     client.execute_sql(f'REMOVE {stage_file_path}')


    def state(self) -> TLoadJobState:
        return "completed"

    def exception(self) -> str:
        raise NotImplementedError()

class DatabricksStagingCopyJob(SqlStagingCopyJob):

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


class DatabricksClient(SqlJobClientWithStaging):
    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, schema: Schema, config: DatabricksClientConfiguration) -> None:
        sql_client = DatabricksSqlClient(
            config.normalize_dataset_name(schema),
            config.credentials
        )
        super().__init__(schema, config, sql_client)
        self.config: DatabricksClientConfiguration = config
        self.sql_client: DatabricksSqlClient = sql_client  # type: ignore
        self.type_mapper = DatabricksTypeMapper(self.capabilities)

    def start_file_load(self, table: TTableSchema, file_path: str, load_id: str) -> LoadJob:
        job = super().start_file_load(table, file_path, load_id)

        if not job:
            job = DatabricksLoadJob(
                file_path,
                table['name'],
                load_id,
                self.sql_client,
                stage_name=self.config.stage_name,
                keep_staged_files=self.config.keep_staged_files,
                staging_credentials=self.config.staging_config.credentials if self.config.staging_config else None
            )
        return job

    def restore_file_load(self, file_path: str) -> LoadJob:
        return EmptyLoadJob.from_file_path(file_path, "completed")

    def _make_add_column_sql(self, new_columns: Sequence[TColumnSchema]) -> List[str]:
        # Override because databricks requires multiple columns in a single ADD COLUMN clause
        return ["ADD COLUMN\n" + ",\n".join(self._get_column_def_sql(c) for c in new_columns)]

    def _create_optimized_replace_job(self, table_chain: Sequence[TTableSchema]) -> NewLoadJob:
        return DatabricksStagingCopyJob.from_table_chain(table_chain, self.sql_client)

    def _get_table_update_sql(self, table_name: str, new_columns: Sequence[TColumnSchema], generate_alter: bool, separate_alters: bool = False) -> List[str]:
        sql = super()._get_table_update_sql(table_name, new_columns, generate_alter)

        cluster_list = [self.capabilities.escape_identifier(c['name']) for c in new_columns if c.get('cluster')]

        if cluster_list:
            sql[0] = sql[0] + "\nCLUSTER BY (" + ",".join(cluster_list) + ")"

        return sql

    def _execute_schema_update_sql(self, only_tables: Iterable[str]) -> TSchemaTables:
        sql_scripts, schema_update = self._build_schema_update_sql(only_tables)
        # stay within max query size when doing DDL. some db backends use bytes not characters so decrease limit by half
        # assuming that most of the characters in DDL encode into single bytes
        self.sql_client.execute_fragments(sql_scripts)
        self._update_schema_in_storage(self.schema)
        return schema_update

    def _from_db_type(self, bq_t: str, precision: Optional[int], scale: Optional[int]) -> TColumnType:
        return self.type_mapper.from_db_type(bq_t, precision, scale)

    def _get_column_def_sql(self, c: TColumnSchema) -> str:
        name = self.capabilities.escape_identifier(c["name"])
        return f"{name} {self.type_mapper.to_db_type(c)} {self._gen_not_null(c.get('nullable', True))}"

    def get_storage_table(self, table_name: str) -> Tuple[bool, TTableSchemaColumns]:
        exists, table = super().get_storage_table(table_name)
        if not exists:
            return exists, table
        table = {col_name.lower(): dict(col, name=col_name.lower()) for col_name, col in table.items()}  # type: ignore
        return exists, table
