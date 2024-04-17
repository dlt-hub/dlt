from typing import ClassVar, Optional, Sequence, Tuple, List, Any
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
    AzureCredentialsWithoutDefaults,
)
from dlt.common.data_types import TDataType
from dlt.common.storages.file_storage import FileStorage
from dlt.common.schema import TColumnSchema, Schema, TTableSchemaColumns
from dlt.common.schema.typing import TTableSchema, TColumnType, TTableFormat


from dlt.destinations.job_client_impl import SqlJobClientWithStaging
from dlt.destinations.job_impl import EmptyLoadJob
from dlt.destinations.exceptions import LoadJobTerminalException

from dlt.destinations.impl.snowflake import capabilities
from dlt.destinations.impl.snowflake.configuration import SnowflakeClientConfiguration
from dlt.destinations.impl.snowflake.sql_client import SnowflakeSqlClient
from dlt.destinations.sql_jobs import SqlJobParams
from dlt.destinations.impl.snowflake.sql_client import SnowflakeSqlClient
from dlt.destinations.job_impl import NewReferenceJob
from dlt.destinations.sql_client import SqlClientBase
from dlt.destinations.type_mapping import TypeMapper


class SnowflakeTypeMapper(TypeMapper):
    BIGINT_PRECISION = 19
    sct_to_unbound_dbt = {
        "complex": "VARIANT",
        "text": "VARCHAR",
        "double": "FLOAT",
        "bool": "BOOLEAN",
        "date": "DATE",
        "timestamp": "TIMESTAMP_TZ",
        "bigint": f"NUMBER({BIGINT_PRECISION},0)",  # Snowflake has no integer types
        "binary": "BINARY",
        "time": "TIME",
    }

    sct_to_dbt = {
        "text": "VARCHAR(%i)",
        "timestamp": "TIMESTAMP_TZ(%i)",
        "decimal": "NUMBER(%i,%i)",
        "time": "TIME(%i)",
        "wei": "NUMBER(%i,%i)",
    }

    dbt_to_sct = {
        "VARCHAR": "text",
        "FLOAT": "double",
        "BOOLEAN": "bool",
        "DATE": "date",
        "TIMESTAMP_TZ": "timestamp",
        "BINARY": "binary",
        "VARIANT": "complex",
        "TIME": "time",
    }

    def from_db_type(
        self, db_type: str, precision: Optional[int] = None, scale: Optional[int] = None
    ) -> TColumnType:
        if db_type == "NUMBER":
            if precision == self.BIGINT_PRECISION and scale == 0:
                return dict(data_type="bigint")
            elif (precision, scale) == self.capabilities.wei_precision:
                return dict(data_type="wei")
            return dict(data_type="decimal", precision=precision, scale=scale)
        return super().from_db_type(db_type, precision, scale)


class SnowflakeLoadJob(LoadJob, FollowupJob):
    def __init__(
        self,
        file_path: str,
        table_name: str,
        load_id: str,
        client: SnowflakeSqlClient,
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
        stage_file_path = ""

        if bucket_path:
            bucket_url = urlparse(bucket_path)
            bucket_scheme = bucket_url.scheme
            # referencing an external s3/azure stage does not require explicit AWS credentials
            if bucket_scheme in ["s3", "az", "abfs"] and stage_name:
                from_clause = f"FROM '@{stage_name}'"
                files_clause = f"FILES = ('{bucket_url.path.lstrip('/')}')"
            # referencing an staged files via a bucket URL requires explicit AWS credentials
            elif (
                bucket_scheme == "s3"
                and staging_credentials
                and isinstance(staging_credentials, AwsCredentialsWithoutDefaults)
            ):
                credentials_clause = f"""CREDENTIALS=(AWS_KEY_ID='{staging_credentials.aws_access_key_id}' AWS_SECRET_KEY='{staging_credentials.aws_secret_access_key}')"""
                from_clause = f"FROM '{bucket_path}'"
            elif (
                bucket_scheme in ["az", "abfs"]
                and staging_credentials
                and isinstance(staging_credentials, AzureCredentialsWithoutDefaults)
            ):
                # Explicit azure credentials are needed to load from bucket without a named stage
                credentials_clause = f"CREDENTIALS=(AZURE_SAS_TOKEN='?{staging_credentials.azure_storage_sas_token}')"
                # Converts an az://<container_name>/<path> to azure://<storage_account_name>.blob.core.windows.net/<container_name>/<path>
                # as required by snowflake
                _path = "/" + bucket_url.netloc + bucket_url.path
                bucket_path = urlunparse(
                    bucket_url._replace(
                        scheme="azure",
                        netloc=f"{staging_credentials.azure_storage_account_name}.blob.core.windows.net",
                        path=_path,
                    )
                )
                from_clause = f"FROM '{bucket_path}'"
            else:
                # ensure that gcs bucket path starts with gcs://, this is a requirement of snowflake
                bucket_path = bucket_path.replace("gs://", "gcs://")
                if not stage_name:
                    # when loading from bucket stage must be given
                    raise LoadJobTerminalException(
                        file_path,
                        f"Cannot load from bucket path {bucket_path} without a stage name. See"
                        " https://dlthub.com/docs/dlt-ecosystem/destinations/snowflake for"
                        " instructions on setting up the `stage_name`",
                    )
                from_clause = f"FROM @{stage_name}/"
                files_clause = f"FILES = ('{urlparse(bucket_path).path.lstrip('/')}')"
        else:
            # this means we have a local file
            if not stage_name:
                # Use implicit table stage by default: "SCHEMA_NAME"."%TABLE_NAME"
                stage_name = client.make_qualified_table_name("%" + table_name)
            stage_file_path = f'@{stage_name}/"{load_id}"/{file_name}'
            from_clause = f"FROM {stage_file_path}"

        # decide on source format, stage_file_path will either be a local file or a bucket path
        source_format = "( TYPE = 'JSON', BINARY_FORMAT = 'BASE64' )"
        if file_name.endswith("parquet"):
            source_format = "(TYPE = 'PARQUET', BINARY_AS_TEXT = FALSE, USE_LOGICAL_TYPE = TRUE)"

        with client.begin_transaction():
            # PUT and COPY in one tx if local file, otherwise only copy
            if not bucket_path:
                client.execute_sql(
                    f'PUT file://{file_path} @{stage_name}/"{load_id}" OVERWRITE = TRUE,'
                    " AUTO_COMPRESS = FALSE"
                )
            client.execute_sql(f"""COPY INTO {qualified_table_name}
                {from_clause}
                {files_clause}
                {credentials_clause}
                FILE_FORMAT = {source_format}
                MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE'
                """)
            if stage_file_path and not keep_staged_files:
                client.execute_sql(f"REMOVE {stage_file_path}")

    def state(self) -> TLoadJobState:
        return "completed"

    def exception(self) -> str:
        raise NotImplementedError()


class SnowflakeClient(SqlJobClientWithStaging, SupportsStagingDestination):
    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, schema: Schema, config: SnowflakeClientConfiguration) -> None:
        sql_client = SnowflakeSqlClient(config.normalize_dataset_name(schema), config.credentials)
        super().__init__(schema, config, sql_client)
        self.config: SnowflakeClientConfiguration = config
        self.sql_client: SnowflakeSqlClient = sql_client  # type: ignore
        self.type_mapper = SnowflakeTypeMapper(self.capabilities)

    def start_file_load(self, table: TTableSchema, file_path: str, load_id: str) -> LoadJob:
        job = super().start_file_load(table, file_path, load_id)

        if not job:
            job = SnowflakeLoadJob(
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

    def _make_add_column_sql(
        self, new_columns: Sequence[TColumnSchema], table_format: TTableFormat = None
    ) -> List[str]:
        # Override because snowflake requires multiple columns in a single ADD COLUMN clause
        return [
            "ADD COLUMN\n"
            + ",\n".join(self._get_column_def_sql(c, table_format) for c in new_columns)
        ]

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

    def _from_db_type(
        self, bq_t: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        return self.type_mapper.from_db_type(bq_t, precision, scale)

    def _get_column_def_sql(self, c: TColumnSchema, table_format: TTableFormat = None) -> str:
        name = self.capabilities.escape_identifier(c["name"])
        return (
            f"{name} {self.type_mapper.to_db_type(c)} {self._gen_not_null(c.get('nullable', True))}"
        )

    def get_storage_table(self, table_name: str) -> Tuple[bool, TTableSchemaColumns]:
        table_name = table_name.upper()  # All snowflake tables are uppercased in information schema
        exists, table = super().get_storage_table(table_name)
        if not exists:
            return exists, table
        # Snowflake converts all unquoted columns to UPPER CASE
        # Convert back to lower case to enable comparison with dlt schema
        table = {col_name.lower(): dict(col, name=col_name.lower()) for col_name, col in table.items()}  # type: ignore
        return exists, table
