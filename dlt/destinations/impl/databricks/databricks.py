from typing import ClassVar, Dict, Optional, Sequence, Tuple, List, Any, Iterable, Type
from urllib.parse import urlparse, urlunparse

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import (
    FollowupJob,
    NewLoadJob,
    TLoadJobState,
    LoadJob,
    CredentialsConfiguration,
    SupportsStagingDestination,
)
from dlt.common.configuration.specs import (
    AwsCredentialsWithoutDefaults,
    AzureCredentials,
    AzureCredentialsWithoutDefaults,
)
from dlt.common.data_types import TDataType
from dlt.common.storages.file_storage import FileStorage
from dlt.common.schema import TColumnSchema, Schema, TTableSchemaColumns
from dlt.common.schema.typing import TTableSchema, TColumnType, TSchemaTables, TTableFormat


from dlt.destinations.insert_job_client import InsertValuesJobClient
from dlt.destinations.job_impl import EmptyLoadJob
from dlt.destinations.exceptions import LoadJobTerminalException

from dlt.destinations.impl.databricks import capabilities
from dlt.destinations.impl.databricks.configuration import DatabricksClientConfiguration
from dlt.destinations.impl.databricks.sql_client import DatabricksSqlClient
from dlt.destinations.sql_jobs import SqlStagingCopyJob, SqlMergeJob, SqlJobParams
from dlt.destinations.job_impl import NewReferenceJob
from dlt.destinations.sql_client import SqlClientBase
from dlt.destinations.type_mapping import TypeMapper


class DatabricksTypeMapper(TypeMapper):
    sct_to_unbound_dbt = {
        "complex": "STRING",  # Databricks supports complex types like ARRAY
        "text": "STRING",
        "double": "DOUBLE",
        "bool": "BOOLEAN",
        "date": "DATE",
        "timestamp": "TIMESTAMP",  # TIMESTAMP for local timezone
        "bigint": "BIGINT",
        "binary": "BINARY",
        "decimal": "DECIMAL",  # DECIMAL(p,s) format
        "time": "STRING",
    }

    dbt_to_sct = {
        "STRING": "text",
        "DOUBLE": "double",
        "BOOLEAN": "bool",
        "DATE": "date",
        "TIMESTAMP": "timestamp",
        "BIGINT": "bigint",
        "INT": "bigint",
        "SMALLINT": "bigint",
        "TINYINT": "bigint",
        "BINARY": "binary",
        "DECIMAL": "decimal",
    }

    sct_to_dbt = {
        "decimal": "DECIMAL(%i,%i)",
        "wei": "DECIMAL(%i,%i)",
    }

    def to_db_integer_type(
        self, precision: Optional[int], table_format: TTableFormat = None
    ) -> str:
        if precision is None:
            return "BIGINT"
        if precision <= 8:
            return "TINYINT"
        if precision <= 16:
            return "SMALLINT"
        if precision <= 32:
            return "INT"
        return "BIGINT"

    def from_db_type(
        self, db_type: str, precision: Optional[int] = None, scale: Optional[int] = None
    ) -> TColumnType:
        # precision and scale arguments here are meaningless as they're not included separately in information schema
        # We use full_data_type from databricks which is either in form "typename" or "typename(precision, scale)"
        type_parts = db_type.split("(")
        if len(type_parts) > 1:
            db_type = type_parts[0]
            scale_str = type_parts[1].strip(")")
            precision, scale = [int(val) for val in scale_str.split(",")]
        else:
            scale = precision = None
        db_type = db_type.upper()
        if db_type == "DECIMAL":
            if (precision, scale) == self.wei_precision():
                return dict(data_type="wei", precision=precision, scale=scale)
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
        staging_credentials: Optional[CredentialsConfiguration] = None,
    ) -> None:
        file_name = FileStorage.get_file_name_from_file_path(file_path)
        super().__init__(file_name)

        qualified_table_name = client.make_qualified_table_name(table_name)

        # extract and prepare some vars
        bucket_path = (
            NewReferenceJob.resolve_reference(file_path)
            if NewReferenceJob.is_reference_job(file_path)
            else ""
        )
        file_name = (
            FileStorage.get_file_name_from_file_path(bucket_path) if bucket_path else file_name
        )
        from_clause = ""
        credentials_clause = ""
        files_clause = ""
        # stage_file_path = ""
        format_options = ""
        copy_options = "COPY_OPTIONS ('mergeSchema'='true')"

        if bucket_path:
            bucket_url = urlparse(bucket_path)
            bucket_scheme = bucket_url.scheme
            # referencing an external s3/azure stage does not require explicit credentials
            if bucket_scheme in ["s3", "az", "abfs", "gc", "gcs"] and stage_name:
                from_clause = f"FROM ('{bucket_path}')"
            # referencing an staged files via a bucket URL requires explicit AWS credentials
            elif (
                bucket_scheme == "s3"
                and staging_credentials
                and isinstance(staging_credentials, AwsCredentialsWithoutDefaults)
            ):
                s3_creds = staging_credentials.to_session_credentials()
                credentials_clause = f"""WITH(CREDENTIAL(
                AWS_ACCESS_KEY='{s3_creds["aws_access_key_id"]}',
                AWS_SECRET_KEY='{s3_creds["aws_secret_access_key"]}',
                AWS_SESSION_TOKEN='{s3_creds["aws_session_token"]}'
                ))
                """
                from_clause = f"FROM '{bucket_path}'"
            elif (
                bucket_scheme in ["az", "abfs"]
                and staging_credentials
                and isinstance(staging_credentials, AzureCredentialsWithoutDefaults)
            ):
                # Explicit azure credentials are needed to load from bucket without a named stage
                credentials_clause = f"""WITH(CREDENTIAL(AZURE_SAS_TOKEN='{staging_credentials.azure_storage_sas_token}'))"""
                # Converts an az://<container_name>/<path> to abfss://<container_name>@<storage_account_name>.dfs.core.windows.net/<path>
                # as required by snowflake
                _path = bucket_url.path
                bucket_path = urlunparse(
                    bucket_url._replace(
                        scheme="abfss",
                        netloc=f"{bucket_url.netloc}@{staging_credentials.azure_storage_account_name}.dfs.core.windows.net",
                        path=_path,
                    )
                )
                from_clause = f"FROM '{bucket_path}'"
            else:
                raise LoadJobTerminalException(
                    file_path,
                    f"Databricks cannot load data from staging bucket {bucket_path}. Only s3 and azure buckets are supported",
                )
        else:
            raise LoadJobTerminalException(
                file_path,
                "Cannot load from local file. Databricks does not support loading from local files. Configure staging with an s3 or azure storage bucket.",
            )

        # decide on source format, stage_file_path will either be a local file or a bucket path
        source_format = "JSON"
        if file_name.endswith("parquet"):
            source_format = "PARQUET"

        statement = f"""COPY INTO {qualified_table_name}
            {from_clause}
            {files_clause}
            {credentials_clause}
            FILEFORMAT = {source_format}
            {format_options}
            {copy_options}
            """
        client.execute_sql(statement)
        # Databricks does not support deleting staged files via sql
        # if stage_file_path and not keep_staged_files:
        #     client.execute_sql(f'REMOVE {stage_file_path}')

    def state(self) -> TLoadJobState:
        return "completed"

    def exception(self) -> str:
        raise NotImplementedError()


class DatabricksStagingCopyJob(SqlStagingCopyJob):
    @classmethod
    def generate_sql(
        cls,
        table_chain: Sequence[TTableSchema],
        sql_client: SqlClientBase[Any],
        params: Optional[SqlJobParams] = None,
    ) -> List[str]:
        sql: List[str] = []
        for table in table_chain:
            with sql_client.with_staging_dataset(staging=True):
                staging_table_name = sql_client.make_qualified_table_name(table["name"])
            table_name = sql_client.make_qualified_table_name(table["name"])
            sql.append(f"DROP TABLE IF EXISTS {table_name};")
            # recreate destination table with data cloned from staging table
            sql.append(f"CREATE TABLE {table_name} CLONE {staging_table_name};")
        return sql


class DatabricksMergeJob(SqlMergeJob):
    @classmethod
    def _to_temp_table(cls, select_sql: str, temp_table_name: str) -> str:
        return f"CREATE TEMPORARY VIEW {temp_table_name} AS {select_sql};"

    @classmethod
    def gen_delete_from_sql(
        cls, table_name: str, column_name: str, temp_table_name: str, temp_table_column: str
    ) -> str:
        # Databricks does not support subqueries in DELETE FROM statements so we use a MERGE statement instead
        return f"""MERGE INTO {table_name}
        USING {temp_table_name}
        ON {table_name}.{column_name} = {temp_table_name}.{temp_table_column}
        WHEN MATCHED THEN DELETE;
        """


class DatabricksClient(InsertValuesJobClient, SupportsStagingDestination):
    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, schema: Schema, config: DatabricksClientConfiguration) -> None:
        sql_client = DatabricksSqlClient(config.normalize_dataset_name(schema), config.credentials)
        super().__init__(schema, config, sql_client)
        self.config: DatabricksClientConfiguration = config
        self.sql_client: DatabricksSqlClient = sql_client
        self.type_mapper = DatabricksTypeMapper(self.capabilities)

    def start_file_load(self, table: TTableSchema, file_path: str, load_id: str) -> LoadJob:
        job = super().start_file_load(table, file_path, load_id)

        if not job:
            job = DatabricksLoadJob(
                file_path,
                table["name"],
                load_id,
                self.sql_client,
                stage_name=self.config.stage_name,
                keep_staged_files=self.config.keep_staged_files,
                staging_credentials=(
                    self.config.staging_config.credentials if self.config.staging_config else None
                ),
            )
        return job

    def restore_file_load(self, file_path: str) -> LoadJob:
        return EmptyLoadJob.from_file_path(file_path, "completed")

    def _create_merge_job(self, table_chain: Sequence[TTableSchema]) -> NewLoadJob:
        return DatabricksMergeJob.from_table_chain(table_chain, self.sql_client)

    def _create_merge_followup_jobs(self, table_chain: Sequence[TTableSchema]) -> List[NewLoadJob]:
        return [DatabricksMergeJob.from_table_chain(table_chain, self.sql_client)]

    # def _create_staging_copy_job(self, table_chain: Sequence[TTableSchema]) -> NewLoadJob:
    #     return DatabricksStagingCopyJob.from_table_chain(table_chain, self.sql_client)

    def _make_add_column_sql(
        self, new_columns: Sequence[TColumnSchema], table_format: TTableFormat = None
    ) -> List[str]:
        # Override because databricks requires multiple columns in a single ADD COLUMN clause
        return ["ADD COLUMN\n" + ",\n".join(self._get_column_def_sql(c) for c in new_columns)]

    # def _create_optimized_replace_job(self, table_chain: Sequence[TTableSchema]) -> NewLoadJob:
    #     return DatabricksStagingCopyJob.from_table_chain(table_chain, self.sql_client)

    def _create_replace_followup_jobs(
        self, table_chain: Sequence[TTableSchema]
    ) -> List[NewLoadJob]:
        if self.config.replace_strategy == "staging-optimized":
            return [DatabricksStagingCopyJob.from_table_chain(table_chain, self.sql_client)]
        return super()._create_replace_followup_jobs(table_chain)

    def _get_table_update_sql(
        self,
        table_name: str,
        new_columns: Sequence[TColumnSchema],
        generate_alter: bool,
        separate_alters: bool = False,
    ) -> List[str]:
        sql = super()._get_table_update_sql(table_name, new_columns, generate_alter)

        cluster_list = [
            self.capabilities.escape_identifier(c["name"]) for c in new_columns if c.get("cluster")
        ]

        if cluster_list:
            sql[0] = sql[0] + "\nCLUSTER BY (" + ",".join(cluster_list) + ")"

        return sql

    def _execute_schema_update_sql(self, only_tables: Iterable[str]) -> TSchemaTables:
        sql_scripts, schema_update = self._build_schema_update_sql(only_tables)
        # stay within max query size when doing DDL. some db backends use bytes not characters so decrease limit by half
        # assuming that most of the characters in DDL encode into single bytes
        self.sql_client.execute_many(sql_scripts)
        self._update_schema_in_storage(self.schema)
        return schema_update

    def _from_db_type(
        self, bq_t: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        return self.type_mapper.from_db_type(bq_t, precision, scale)

    def _get_column_def_sql(self, c: TColumnSchema, table_format: TTableFormat = None) -> str:
        name = self.capabilities.escape_identifier(c["name"])
        return (
            f"{name} {self.type_mapper.to_db_type(c)} {self._gen_not_null(c.get('nullable', True))}"
        )

    def _get_storage_table_query_columns(self) -> List[str]:
        fields = super()._get_storage_table_query_columns()
        fields[
            1
        ] = "full_data_type"  # Override because this is the only way to get data type with precision
        return fields
