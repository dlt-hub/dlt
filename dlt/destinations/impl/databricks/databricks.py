import os
from typing import Dict, Optional, Sequence, List, cast, Union
from urllib.parse import urlparse
from pathlib import Path

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
    TABLE_PROPERTIES_HINT,
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
from dlt.common.exceptions import TerminalValueError
from dlt.destinations.job_client_impl import SqlJobClientWithStagingDataset
from dlt.destinations.exceptions import LoadJobTerminalException
from dlt.destinations.impl.databricks.configuration import DatabricksClientConfiguration
from dlt.destinations.impl.databricks.sql_client import DatabricksSqlClient
from dlt.destinations.sql_jobs import SqlMergeFollowupJob
from dlt.destinations.job_impl import ReferenceFollowupJobRequest
from dlt.destinations.impl.databricks.typing import TDatabricksColumnHint
from dlt.destinations.path_utils import get_file_format_and_compression

SUPPORTED_BLOB_STORAGE_PROTOCOLS = AZURE_BLOB_STORAGE_PROTOCOLS + S3_PROTOCOLS + GCS_PROTOCOLS


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
            # staging_allowed_local_path should be set when opening the connection but at that
            # time we do not know this path so do it now
            conn_ = self._sql_client.native_connection
            file_dir = os.path.dirname(self._file_path)
            if backend := getattr(conn_, "thrift_backend", None):
                backend.staging_allowed_local_path = file_dir
            else:
                # thrift backend discontinued on newer databricks connector clients
                conn_.staging_allowed_local_path = file_dir  # type: ignore[attr-defined,unused-ignore]
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

        posix_path = Path(
            local_file_path
        ).as_posix()  # backslash in Windows local path causes issues with PUT command
        self._sql_client.execute_sql(f"PUT '{posix_path}' INTO '{volume_file_path}' OVERWRITE")

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
        dataset_name, staging_dataset_name = SqlJobClientWithStagingDataset.create_dataset_names(
            schema, config
        )
        sql_client = DatabricksSqlClient(
            dataset_name,
            staging_dataset_name,
            config.credentials,
            capabilities,
        )
        super().__init__(schema, config, sql_client)
        self.config: DatabricksClientConfiguration = config
        self.sql_client: DatabricksSqlClient = sql_client  # type: ignore[assignment, unused-ignore]
        self.type_mapper = self.capabilities.get_type_mapper()
        # PK and FK are created in SQL fragments, not inline

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
        self, table_name: str, new_columns: Sequence[TColumnSchema], generate_alter: bool
    ) -> List[str]:
        table = self.prepare_load_table(table_name)

        # Check for AUTO clustering at table level
        cluster_by_auto = table.get(CLUSTER_HINT) == "AUTO"

        # Get cluster columns from column hints
        cluster_list = [
            self.sql_client.escape_column_name(c["name"])
            for c in new_columns
            if c.get("cluster") or c.get(CLUSTER_HINT, False)
        ]

        # Get partition columns from column hints
        partition_list = [
            self.sql_client.escape_column_name(c["name"])
            for c in new_columns
            if c.get("partition", False)
        ]

        # Determine the CLUSTER BY clause
        cluster_clause = None
        if cluster_by_auto:
            cluster_clause = "CLUSTER BY AUTO"
        elif cluster_list:
            cluster_clause = f"CLUSTER BY ({','.join(cluster_list)})"

        # Determine the PARTITIONED BY clause
        partition_clause = None
        if partition_list:
            partition_clause = f"PARTITIONED BY ({','.join(partition_list)})"

        # Get table properties
        table_properties = table.get(TABLE_PROPERTIES_HINT)
        tblproperties_clause = None
        if table_properties and isinstance(table_properties, dict):
            props = []
            for key, value in table_properties.items():
                # Escape key and value properly
                escaped_key = f"'{key}'"
                if isinstance(value, str):
                    escaped_value = f"'{value}'"
                elif isinstance(value, bool):
                    escaped_value = str(value).lower()
                else:
                    escaped_value = str(value)
                props.append(f"{escaped_key}={escaped_value}")
            tblproperties_clause = f"TBLPROPERTIES ({', '.join(props)})"

        # Get table format
        table_format = table.get("table_format", "delta")
        using_clause = None
        if table_format == "iceberg":
            using_clause = "USING ICEBERG"

            # Validate Iceberg-specific constraints
            if table_properties and isinstance(table_properties, dict):
                # Check for Delta-specific properties that are not supported in Iceberg
                delta_only_props = [
                    "delta.dataSkippingStatsColumns",
                    "delta.autoOptimize.optimizeWrite",
                    "delta.autoOptimize.autoCompact",
                    "delta.logRetentionDuration",
                    "delta.deletedFileRetentionDuration",
                    "delta.enableChangeDataFeed",
                    "delta.columnMapping.mode",
                    "delta.appendOnly",
                ]

                for prop_key in table_properties.keys():
                    if any(
                        prop_key.startswith(delta_prop) or prop_key == delta_prop
                        for delta_prop in delta_only_props
                    ):
                        raise TerminalValueError(
                            f"Table property '{prop_key}' is Delta Lake specific and not supported"
                            " with ICEBERG tables. Remove this property when using"
                            " table_format='iceberg'."
                        )
        # Note: DELTA is the default format, no explicit USING clause needed

        # For CREATE TABLE, we need custom generation if we have any custom clauses or non-DELTA format
        if not generate_alter and (
            cluster_clause or partition_clause or tblproperties_clause or using_clause
        ):
            # Build CREATE TABLE with all custom clauses
            qualified_name = self.sql_client.make_qualified_table_name(table_name)
            sql = self._make_create_table(qualified_name, table)

            sql += " (\n"
            sql += ",\n".join([self._get_column_def_sql(c, table) for c in new_columns])
            sql += self._get_constraints_sql(table_name, new_columns, generate_alter)
            sql += ")"

            # Add USING clause after column definitions for ICEBERG
            if using_clause:
                sql += f" {using_clause}"

            # Add PARTITIONED BY clause (must come before CLUSTER BY)
            if partition_clause:
                sql += f" {partition_clause}"
            # Add CLUSTER BY clause
            if cluster_clause:
                sql += f" {cluster_clause}"
            # Add TBLPROPERTIES clause (comes after CLUSTER BY)
            if tblproperties_clause:
                sql += f" {tblproperties_clause}"
            sql_result = [sql]
        else:
            # Use parent implementation for ALTER or non-clustered/non-partitioned tables
            sql_result = super()._get_table_update_sql(table_name, new_columns, generate_alter)

            # For ALTER TABLE, add CLUSTER BY as a separate statement
            # Note: PARTITIONED BY cannot be added via ALTER TABLE in Databricks
            if generate_alter and cluster_clause:
                qualified_name = self.sql_client.make_qualified_table_name(table_name)
                sql_result.append(f"ALTER TABLE {qualified_name} {cluster_clause}")

        qualified_name = self.sql_client.make_qualified_table_name(table_name)

        if table.get(TABLE_COMMENT_HINT) or table.get("description"):
            comment = table.get(TABLE_COMMENT_HINT) or table.get("description")
            escaped_comment = escape_databricks_literal(comment)
            sql_result.append(f"COMMENT ON TABLE {qualified_name} IS {escaped_comment}")

        if table.get(TABLE_TAGS_HINT):
            table_tags = cast(List[Union[str, Dict[str, str]]], table.get(TABLE_TAGS_HINT))
            for tag in table_tags:
                if isinstance(tag, str):
                    escaped_tag = escape_databricks_literal(tag)
                    sql_result.append(f"ALTER TABLE {qualified_name} SET TAGS ({escaped_tag})")
                elif isinstance(tag, dict):
                    (key, value), *rest = tag.items()
                    escaped_key = escape_databricks_literal(key)
                    escaped_value = escape_databricks_literal(value)
                    sql_result.append(
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
                        sql_result.append(
                            f"ALTER TABLE {qualified_name} ALTER COLUMN {column_name} SET TAGS"
                            f" ({escaped_tag})"
                        )
                    elif isinstance(column_tag, dict):
                        (key, value), *rest = column_tag.items()
                        escaped_key = escape_databricks_literal(key)
                        escaped_value = escape_databricks_literal(value)
                        sql_result.append(
                            f"ALTER TABLE {qualified_name} ALTER COLUMN {column_name} SET TAGS"
                            f" ({escaped_key}={escaped_value})"
                        )

        return sql_result

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
