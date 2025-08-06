import os
from typing import Dict, Optional, Sequence, List, cast, Union
from urllib.parse import urlparse

from dlt.common.configuration.specs.azure_credentials import (
    AzureServicePrincipalCredentialsWithoutDefaults,
)
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.client import (
    HasFollowupJobs,
    FollowupJobRequest,
    PreparedTableSchema,
    RunnableLoadJob,
    SupportsStagingDestination,
    LoadJob,
)
from dlt.common.configuration.specs import (
    AwsCredentialsWithoutDefaults,
    AzureCredentialsWithoutDefaults,
)
from dlt.common.schema.utils import get_columns_names_with_prop
from dlt.common.storages.configuration import ensure_canonical_az_url
from dlt.common.storages.file_storage import FileStorage
from dlt.common.storages.fsspec_filesystem import (
    AZURE_BLOB_STORAGE_PROTOCOLS,
    S3_PROTOCOLS,
    GCS_PROTOCOLS,
)
from dlt.destinations.impl.databricks.databricks_adapter import (
    CLUSTER_HINT,
    TABLE_COMMENT_HINT,
    TABLE_TAGS_HINT,
    COLUMN_COMMENT_HINT,
    COLUMN_TAGS_HINT,
)
from dlt.common.schema import TColumnSchema, Schema, TTableSchema, TColumnHint
from dlt.common.schema.typing import TColumnType
from dlt.common.storages import FilesystemConfiguration, fsspec_from_config
from dlt.common.utils import uniq_id
from dlt.common import logger
from dlt.common.data_writers.escape import escape_databricks_literal
from dlt.destinations.job_client_impl import SqlJobClientWithStagingDataset
from dlt.destinations.exceptions import LoadJobTerminalException
from dlt.destinations.impl.databricks.configuration import DatabricksClientConfiguration
from dlt.destinations.impl.databricks.sql_client import DatabricksSqlClient
from dlt.destinations.sql_jobs import SqlMergeFollowupJob
from dlt.destinations.job_impl import ReferenceFollowupJobRequest
from dlt.destinations.impl.databricks.typing import TDatabricksColumnHint
from dlt.destinations.path_utils import get_file_format_and_compression

SUPPORTED_BLOB_STORAGE_PROTOCOLS = AZURE_BLOB_STORAGE_PROTOCOLS + S3_PROTOCOLS + GCS_PROTOCOLS


SUPPORTED_HINTS: Dict[TDatabricksColumnHint, str] = {
    "primary_key": "PRIMARY KEY",
    "foreign_key": "FOREIGN KEY",
}


class DatabricksLoadJob(RunnableLoadJob, HasFollowupJobs):
    def __init__(
        self,
        file_path: str,
        staging_config: FilesystemConfiguration,
    ) -> None:
        super().__init__(file_path)
        self._staging_config = staging_config
        self._job_client: "DatabricksClient" = None

    def run(self) -> None:
        self._sql_client = self._job_client.sql_client

        qualified_table_name = self._sql_client.make_qualified_table_name(self.load_table_name)

        # decide if this is a local file or a staged file
        is_local_file = not ReferenceFollowupJobRequest.is_reference_job(self._file_path)
        if is_local_file:
            # conn parameter staging_allowed_local_path must be set to use 'PUT/REMOVE volume_path' SQL statement
            self._sql_client.native_connection.thrift_backend.staging_allowed_local_path = (
                os.path.dirname(self._file_path)
            )
            # local file by uploading to a temporary volume on Databricks
            from_clause, file_name, volume_path, volume_file_path = self._handle_local_file_upload(
                self._file_path
            )
            credentials_clause = ""
            orig_bucket_path = None  # not used for local file
        else:
            # staged file
            from_clause, credentials_clause, file_name, orig_bucket_path = (
                self._handle_staged_file()
            )

        # decide on source format, file_name will either be a local file or a bucket path
        source_format, format_options_clause, skip_load = self._determine_source_format(
            file_name, orig_bucket_path
        )

        if skip_load:
            # If the file is empty or otherwise un-loadable, exit early
            return

        statement = self._build_copy_into_statement(
            qualified_table_name,
            from_clause,
            credentials_clause,
            source_format,
            format_options_clause,
        )

        self._sql_client.execute_sql(statement)

        if is_local_file and not self._job_client.config.keep_staged_files:
            self._handle_staged_file_remove(volume_path, volume_file_path)

    def _handle_staged_file_remove(self, volume_path: str, volume_file_path: str) -> None:
        self._sql_client.execute_sql(f"REMOVE '{volume_file_path}'")
        self._sql_client.execute_sql(f"REMOVE '{volume_path}'")

    def _handle_local_file_upload(self, local_file_path: str) -> tuple[str, str, str, str]:
        file_name = FileStorage.get_file_name_from_file_path(local_file_path)
        volume_file_name = file_name
        if file_name.startswith(("_", ".")):
            volume_file_name = (
                "valid" + file_name
            )  # databricks loading fails when file_name starts with - or .

        volume_catalog = self._sql_client.database_name
        volume_database = self._sql_client.dataset_name
        volume_name = "_dlt_staging_load_volume"

        fully_qualified_volume_name = f"{volume_catalog}.{volume_database}.{volume_name}"
        if self._job_client.config.staging_volume_name:
            fully_qualified_volume_name = self._job_client.config.staging_volume_name
            volume_catalog, volume_database, volume_name = fully_qualified_volume_name.split(".")
        else:
            # create staging volume named _dlt_staging_load_volume
            self._sql_client.execute_sql(f"""
                CREATE VOLUME IF NOT EXISTS {fully_qualified_volume_name}
            """)

        volume_path = f"/Volumes/{volume_catalog}/{volume_database}/{volume_name}/{uniq_id()}"
        volume_file_path = f"{volume_path}/{volume_file_name}"

        self._sql_client.execute_sql(f"PUT '{local_file_path}' INTO '{volume_file_path}' OVERWRITE")

        from_clause = f"FROM '{volume_path}'"

        return from_clause, file_name, volume_path, volume_file_path

    def _handle_staged_file(self) -> tuple[str, str, str, str]:
        bucket_path = orig_bucket_path = (
            ReferenceFollowupJobRequest.resolve_reference(self._file_path)
            if ReferenceFollowupJobRequest.is_reference_job(self._file_path)
            else ""
        )

        if not bucket_path:
            raise LoadJobTerminalException(
                self._file_path,
                "Cannot load from local file. Databricks does not support loading from local files."
                " Configure staging with an s3, azure or google storage bucket.",
            )

        file_name = FileStorage.get_file_name_from_file_path(bucket_path)

        staging_credentials = self._staging_config.credentials
        bucket_url = urlparse(bucket_path)
        bucket_scheme = bucket_url.scheme

        if bucket_scheme not in SUPPORTED_BLOB_STORAGE_PROTOCOLS:
            raise LoadJobTerminalException(
                self._file_path,
                f"Databricks cannot load data from staging bucket `{bucket_path}`. "
                "Only s3, azure and gcs buckets are supported. "
                "Please note that gcs buckets are supported only via named credential.",
            )

        credentials_clause = ""

        if self._job_client.config.is_staging_external_location:
            # just skip the credentials clause for external location
            # https://docs.databricks.com/en/sql/language-manual/sql-ref-external-locations.html#external-location
            pass
        elif self._job_client.config.staging_credentials_name:
            # add named credentials
            credentials_clause = (
                f"WITH(CREDENTIAL {self._job_client.config.staging_credentials_name} )"
            )
        else:
            # referencing an staged files via a bucket URL requires explicit AWS credentials
            if bucket_scheme == "s3":
                assert isinstance(staging_credentials, AwsCredentialsWithoutDefaults)
                s3_creds = staging_credentials.to_session_credentials()
                credentials_clause = f"""WITH(CREDENTIAL(
                    AWS_ACCESS_KEY='{s3_creds["aws_access_key_id"]}',
                    AWS_SECRET_KEY='{s3_creds["aws_secret_access_key"]}',
                    AWS_SESSION_TOKEN='{s3_creds["aws_session_token"]}'
                ))"""
            elif bucket_scheme in AZURE_BLOB_STORAGE_PROTOCOLS:
                assert isinstance(
                    staging_credentials, AzureCredentialsWithoutDefaults
                ), "AzureCredentialsWithoutDefaults required to pass explicit credential"
                # Explicit azure credentials are needed to load from bucket without a named stage
                credentials_clause = f"""WITH(CREDENTIAL(AZURE_SAS_TOKEN='{staging_credentials.azure_storage_sas_token}'))"""
                bucket_path = self.ensure_databricks_abfss_url(
                    bucket_path,
                    staging_credentials.azure_storage_account_name,
                    staging_credentials.azure_account_host,
                )
            else:
                raise LoadJobTerminalException(
                    self._file_path,
                    "You need to use Databricks named credential to use google storage."
                    " Passing explicit Google credentials is not supported by Databricks.",
                )

        if bucket_scheme in AZURE_BLOB_STORAGE_PROTOCOLS:
            assert isinstance(
                staging_credentials,
                (AzureCredentialsWithoutDefaults, AzureServicePrincipalCredentialsWithoutDefaults),
            )
            bucket_path = self.ensure_databricks_abfss_url(
                bucket_path,
                staging_credentials.azure_storage_account_name,
                staging_credentials.azure_account_host,
            )

        # always add FROM clause
        from_clause = f"FROM '{bucket_path}'"

        return from_clause, credentials_clause, file_name, orig_bucket_path

    def _determine_source_format(
        self, file_name: str, orig_bucket_path: str
    ) -> tuple[str, str, bool]:
        file_format, _ = get_file_format_and_compression(file_name)

        if file_format == "parquet":
            return "PARQUET", "", False

        elif file_format in ["jsonl", "typed-jsonl"]:
            format_options_clause = "FORMAT_OPTIONS('inferTimestamp'='true')"

            # check for an empty JSON file, unless it's a direct load
            if self._staging_config is not None:
                fs, _ = fsspec_from_config(self._staging_config)
                if orig_bucket_path is not None:
                    file_size = fs.size(orig_bucket_path)
                    if file_size == 0:
                        return "JSON", format_options_clause, True

            return "JSON", format_options_clause, False

        raise LoadJobTerminalException(
            self._file_path, "Databricks loader only supports .parquet or .jsonl file extensions."
        )

    def _build_copy_into_statement(
        self,
        qualified_table_name: str,
        from_clause: str,
        credentials_clause: str,
        source_format: str,
        format_options_clause: str,
    ) -> str:
        return f"""COPY INTO {qualified_table_name}
            {from_clause}
            {credentials_clause}
            FILEFORMAT = {source_format}
            {format_options_clause}
        """

    @staticmethod
    def ensure_databricks_abfss_url(
        bucket_path: str, azure_storage_account_name: str = None, account_host: str = None
    ) -> str:
        return ensure_canonical_az_url(
            bucket_path, "abfss", azure_storage_account_name, account_host
        )


class DatabricksMergeJob(SqlMergeFollowupJob):
    @classmethod
    def _to_temp_table(cls, select_sql: str, temp_table_name: str, unique_column: str) -> str:
        return f"CREATE TEMPORARY VIEW {temp_table_name} AS {select_sql}"

    @classmethod
    def gen_delete_from_sql(
        cls, table_name: str, column_name: str, temp_table_name: str, temp_table_column: str
    ) -> str:
        # Databricks does not support subqueries in DELETE FROM statements so we use a MERGE statement instead
        return f"""MERGE INTO {table_name}
        USING {temp_table_name}
        ON {table_name}.{column_name} = {temp_table_name}.{temp_table_column}
        WHEN MATCHED THEN DELETE
        """


class DatabricksClient(SqlJobClientWithStagingDataset, SupportsStagingDestination):
    def __init__(
        self,
        schema: Schema,
        config: DatabricksClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        sql_client = DatabricksSqlClient(
            config.normalize_dataset_name(schema),
            config.normalize_staging_dataset_name(schema),
            config.credentials,
            capabilities,
        )
        super().__init__(schema, config, sql_client)
        self.config: DatabricksClientConfiguration = config
        self.sql_client: DatabricksSqlClient = sql_client  # type: ignore[assignment, unused-ignore]
        self.type_mapper = self.capabilities.get_type_mapper()
        self.active_hints = (
            cast(Dict[TColumnHint, str], SUPPORTED_HINTS) if self.config.create_indexes else {}
        )

    def _get_column_def_sql(self, column: TColumnSchema, table: PreparedTableSchema = None) -> str:
        column_def_sql = super()._get_column_def_sql(column, table)

        if column.get(COLUMN_COMMENT_HINT) or column.get("description"):
            comment = column.get(COLUMN_COMMENT_HINT) or column.get("description")
            escaped_comment = escape_databricks_literal(comment)
            column_def_sql = f"{column_def_sql} COMMENT {escaped_comment}"
        return column_def_sql

    def create_load_job(
        self, table: PreparedTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        job = super().create_load_job(table, file_path, load_id, restore)

        if not job:
            job = DatabricksLoadJob(
                file_path,
                staging_config=cast(FilesystemConfiguration, self.config.staging_config),
            )
        return job

    def _create_merge_followup_jobs(
        self, table_chain: Sequence[PreparedTableSchema]
    ) -> List[FollowupJobRequest]:
        return [DatabricksMergeJob.from_table_chain(table_chain, self.sql_client)]

    def _make_add_column_sql(
        self, new_columns: Sequence[TColumnSchema], table: PreparedTableSchema = None
    ) -> List[str]:
        # Override because databricks requires multiple columns in a single ADD COLUMN clause
        return [
            "ADD COLUMN\n" + ",\n".join(self._get_column_def_sql(c, table) for c in new_columns)
        ]

    def _get_constraints_sql(
        self,
        table_name: str,
        new_columns: Sequence[TColumnSchema],
        generate_alter: bool,
    ) -> str:
        constraints_sql = ""

        partial: TTableSchema = {
            "name": table_name,
            "columns": {c["name"]: c for c in new_columns},
        }

        if self.config.create_indexes:
            # Adding primary key constraint
            pk_columns = get_columns_names_with_prop(partial, "primary_key")

            if pk_columns:
                logger.info(f"Creating PRIMARY KEY constraint for table {table_name}")
                if generate_alter:
                    logger.warning(
                        f"PRIMARY KEY on {table_name} constraint cannot be added in ALTER TABLE and"
                        " is ignored"
                    )
                else:
                    quoted_pk_cols = ", ".join(
                        self.sql_client.escape_column_name(col) for col in pk_columns
                    )
                    constraints_sql += f",\nPRIMARY KEY ({quoted_pk_cols})"

            return constraints_sql

        return ""

    def _get_table_update_sql(
        self,
        table_name: str,
        new_columns: Sequence[TColumnSchema],
        generate_alter: bool,
        separate_alters: bool = False,
    ) -> List[str]:
        table = self.prepare_load_table(table_name)
        sql = super()._get_table_update_sql(table_name, new_columns, generate_alter)
        qualified_name = self.sql_client.make_qualified_table_name(table_name)

        cluster_list = [
            self.sql_client.escape_column_name(c["name"])
            for c in new_columns
            if c.get("cluster") or c.get(CLUSTER_HINT, False)
        ]

        if cluster_list:
            sql.append(f"ALTER TABLE {qualified_name} CLUSTER BY (" + ",".join(cluster_list) + ")")

        if table.get(TABLE_COMMENT_HINT) or table.get("description"):
            comment = table.get(TABLE_COMMENT_HINT) or table.get("description")
            escaped_comment = escape_databricks_literal(comment)
            sql.append(f"COMMENT ON TABLE {qualified_name} IS {escaped_comment}")

        if table.get(TABLE_TAGS_HINT):
            table_tags = cast(List[Union[str, Dict[str, str]]], table.get(TABLE_TAGS_HINT))
            for tag in table_tags:
                if isinstance(tag, str):
                    escaped_tag = escape_databricks_literal(tag)
                    sql.append(f"ALTER TABLE {qualified_name} SET TAGS ({escaped_tag})")
                elif isinstance(tag, dict):
                    (key, value), *rest = tag.items()
                    escaped_key = escape_databricks_literal(key)
                    escaped_value = escape_databricks_literal(value)
                    sql.append(
                        f"ALTER TABLE {qualified_name} SET TAGS ({escaped_key}={escaped_value})"
                    )

        column_tag_list = [
            (self.sql_client.escape_column_name(c["name"]), c.get(COLUMN_TAGS_HINT))
            for c in new_columns
            if c.get(COLUMN_TAGS_HINT)
        ]

        if column_tag_list:
            for column_name, column_tags in column_tag_list:
                column_tags_typed = cast(List[Union[str, Dict[str, str]]], column_tags)
                for column_tag in column_tags_typed:
                    if isinstance(column_tag, str):
                        escaped_tag = escape_databricks_literal(column_tag)
                        sql.append(
                            f"ALTER TABLE {qualified_name} ALTER COLUMN {column_name} SET TAGS"
                            f" ({escaped_tag})"
                        )
                    elif isinstance(column_tag, dict):
                        (key, value), *rest = column_tag.items()
                        escaped_key = escape_databricks_literal(key)
                        escaped_value = escape_databricks_literal(value)
                        sql.append(
                            f"ALTER TABLE {qualified_name} ALTER COLUMN {column_name} SET TAGS"
                            f" ({escaped_key}={escaped_value})"
                        )

        return sql

    def _get_table_post_update_sql(
        self,
        table: TTableSchema,
    ) -> List[str]:
        sql: List[str] = []
        table_name = table["name"]

        # add foreign key constraints
        references = table.get("references")
        if references:
            logger.info(f"Creating FOREIGN KEY constraint for table {table_name}")
            for reference in references:
                quoted_fk_cols = ", ".join(
                    self.sql_client.escape_column_name(col) for col in reference.get("columns")
                )
                quoted_reference_cols = ", ".join(
                    self.sql_client.escape_column_name(col)
                    for col in reference.get("referenced_columns")
                )
                sql.append(
                    f"ALTER TABLE {self.sql_client.make_qualified_table_name(table_name)} ADD"
                    f" FOREIGN KEY ({quoted_fk_cols}) REFERENCES"
                    f" {self.sql_client.make_qualified_table_name(reference.get('referenced_table'))}({quoted_reference_cols})"
                )

        return sql

    def _from_db_type(
        self, bq_t: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        return self.type_mapper.from_destination_type(bq_t, precision, scale)

    def _get_storage_table_query_columns(self) -> List[str]:
        fields = super()._get_storage_table_query_columns()
        fields[2] = (  # Override because this is the only way to get data type with precision
            "full_data_type"
        )
        return fields

    def should_truncate_table_before_load_on_staging_destination(self, table_name: str) -> bool:
        return self.config.truncate_tables_on_staging_destination_before_load
