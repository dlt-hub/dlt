from typing import Optional, Sequence, List, Dict, Set
from urllib.parse import urlparse, urlunparse

from dlt.common import logger
from dlt.common.data_writers.configuration import CsvFormatConfiguration
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.client import (
    HasFollowupJobs,
    LoadJob,
    PreparedTableSchema,
    RunnableLoadJob,
    CredentialsConfiguration,
    SupportsStagingDestination,
)
from dlt.common.configuration.specs import (
    AwsCredentialsWithoutDefaults,
    AzureCredentialsWithoutDefaults,
)
from dlt.common.schema.utils import get_columns_names_with_prop
from dlt.common.storages.configuration import FilesystemConfiguration, ensure_canonical_az_url
from dlt.common.storages.file_storage import FileStorage
from dlt.common.schema import TColumnSchema, Schema, TColumnHint
from dlt.common.schema.typing import TColumnType, TTableSchema

from dlt.common.storages.fsspec_filesystem import AZURE_BLOB_STORAGE_PROTOCOLS, S3_PROTOCOLS
from dlt.common.typing import TLoaderFileFormat
from dlt.common.utils import uniq_id
from dlt.destinations.job_client_impl import SqlJobClientWithStagingDataset
from dlt.destinations.exceptions import LoadJobTerminalException

from dlt.destinations.impl.snowflake.configuration import SnowflakeClientConfiguration
from dlt.destinations.impl.snowflake.sql_client import SnowflakeSqlClient
from dlt.destinations.job_impl import ReferenceFollowupJobRequest

SUPPORTED_HINTS: Dict[TColumnHint, str] = {"unique": "UNIQUE"}


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
        file_format = file_name.rsplit(".", 1)[-1]

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

        copy_sql = self.gen_copy_sql(
            file_url,
            qualified_table_name,
            file_format,  # type: ignore[arg-type]
            self._sql_client.capabilities.generates_case_sensitive_identifiers(),
            self._stage_name,
            stage_file_path,
            self._staging_credentials,
            self._config.csv_format,
            self._config.use_vectorized_scanner,
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

    @classmethod
    def gen_copy_sql(
        cls,
        file_url: str,
        qualified_table_name: str,
        loader_file_format: TLoaderFileFormat,
        is_case_sensitive: bool,
        stage_name: Optional[str] = None,
        local_stage_file_path: Optional[str] = None,
        staging_credentials: Optional[CredentialsConfiguration] = None,
        csv_format: Optional[CsvFormatConfiguration] = None,
        use_vectorized_scanner: Optional[bool] = False,
    ) -> str:
        parsed_file_url = urlparse(file_url)
        # check if local filesystem (file scheme or just a local file in native form)
        is_local = parsed_file_url.scheme == "file" or FilesystemConfiguration.is_local_path(
            file_url
        )
        # file_name = FileStorage.get_file_name_from_file_path(file_url)

        from_clause = ""
        credentials_clause = ""
        files_clause = ""
        on_error_clause = ""

        case_folding = "CASE_SENSITIVE" if is_case_sensitive else "CASE_INSENSITIVE"
        column_match_clause = f"MATCH_BY_COLUMN_NAME='{case_folding}'"

        if not is_local:
            bucket_scheme = parsed_file_url.scheme
            # referencing an external s3/azure stage does not require explicit AWS credentials
            if bucket_scheme in AZURE_BLOB_STORAGE_PROTOCOLS + S3_PROTOCOLS and stage_name:
                from_clause = f"FROM '@{stage_name}'"
                files_clause = f"FILES = ('{parsed_file_url.path.lstrip('/')}')"
            # referencing an staged files via a bucket URL requires explicit AWS credentials
            elif (
                bucket_scheme in S3_PROTOCOLS
                and staging_credentials
                and isinstance(staging_credentials, AwsCredentialsWithoutDefaults)
            ):
                credentials_clause = f"""CREDENTIALS=(AWS_KEY_ID='{staging_credentials.aws_access_key_id}' AWS_SECRET_KEY='{staging_credentials.aws_secret_access_key}')"""
                from_clause = f"FROM '{file_url}'"
            elif (
                bucket_scheme in AZURE_BLOB_STORAGE_PROTOCOLS
                and staging_credentials
                and isinstance(staging_credentials, AzureCredentialsWithoutDefaults)
            ):
                credentials_clause = f"CREDENTIALS=(AZURE_SAS_TOKEN='?{staging_credentials.azure_storage_sas_token}')"
                file_url = cls.ensure_snowflake_azure_url(
                    file_url,
                    staging_credentials.azure_storage_account_name,
                    staging_credentials.azure_account_host,
                )
                from_clause = f"FROM '{file_url}'"
            else:
                # ensure that gcs bucket path starts with gcs://, this is a requirement of snowflake
                file_url = file_url.replace("gs://", "gcs://")
                if not stage_name:
                    # when loading from bucket stage must be given
                    raise LoadJobTerminalException(
                        file_url,
                        f"Cannot load from bucket path {file_url} without a stage name. See"
                        " https://dlthub.com/docs/dlt-ecosystem/destinations/snowflake for"
                        " instructions on setting up the `stage_name`",
                    )
                from_clause = f"FROM @{stage_name}/"
                files_clause = f"FILES = ('{urlparse(file_url).path.lstrip('/')}')"
        else:
            from_clause = f"FROM {local_stage_file_path}"

        # decide on source format, stage_file_path will either be a local file or a bucket path
        if loader_file_format == "jsonl":
            source_format = "( TYPE = 'JSON', BINARY_FORMAT = 'BASE64' )"
        elif loader_file_format == "parquet":
            source_format = "(TYPE = 'PARQUET', BINARY_AS_TEXT = FALSE, USE_LOGICAL_TYPE = TRUE"
            if use_vectorized_scanner:
                source_format += ", USE_VECTORIZED_SCANNER = TRUE"
                on_error_clause = "ON_ERROR = ABORT_STATEMENT"
            source_format += ")"
        elif loader_file_format == "csv":
            # empty strings are NULL, no data is NULL, missing columns (ERROR_ON_COLUMN_COUNT_MISMATCH) are NULL
            csv_format = csv_format or CsvFormatConfiguration()
            source_format = (
                "(TYPE = 'CSV', BINARY_FORMAT = 'UTF-8', PARSE_HEADER ="
                f" {csv_format.include_header}, FIELD_OPTIONALLY_ENCLOSED_BY = '\"', NULL_IF ="
                " (''), ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE,"
                f" FIELD_DELIMITER='{csv_format.delimiter}', ENCODING='{csv_format.encoding}')"
            )
            # disable column match if headers are not provided
            if not csv_format.include_header:
                column_match_clause = ""
            if csv_format.on_error_continue:
                on_error_clause = "ON_ERROR = CONTINUE"
        else:
            raise ValueError(f"{loader_file_format} not supported for Snowflake COPY command.")

        return f"""COPY INTO {qualified_table_name}
            {from_clause}
            {files_clause}
            {credentials_clause}
            FILE_FORMAT = {source_format}
            {column_match_clause}
            {on_error_clause}
        """

    @staticmethod
    def ensure_snowflake_azure_url(
        file_url: str, account_name: str = None, account_host: str = None
    ) -> str:
        # Explicit azure credentials are needed to load from bucket without a named stage
        if not account_host and account_name:
            account_host = f"{account_name}.blob.core.windows.net"
        # get canonical url first to convert it into snowflake form
        canonical_url = ensure_canonical_az_url(
            file_url,
            "azure",
            account_name,
            account_host,
        )
        parsed_file_url = urlparse(canonical_url)
        return urlunparse(
            parsed_file_url._replace(
                path=f"/{parsed_file_url.username}{parsed_file_url.path}",
                netloc=parsed_file_url.hostname,
            )
        )


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
