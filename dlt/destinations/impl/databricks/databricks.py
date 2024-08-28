from typing import Optional, Sequence, List, cast
from urllib.parse import urlparse, urlunparse

from dlt import config
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import (
    HasFollowupJobs,
    FollowupJobRequest,
    RunnableLoadJob,
    SupportsStagingDestination,
    LoadJob,
)
from dlt.common.configuration.specs import (
    AwsCredentialsWithoutDefaults,
    AzureCredentialsWithoutDefaults,
)
from dlt.common.exceptions import TerminalValueError
from dlt.common.storages.file_storage import FileStorage
from dlt.common.schema import TColumnSchema, Schema
from dlt.common.schema.typing import TTableSchema, TColumnType, TSchemaTables, TTableFormat
from dlt.common.schema.utils import table_schema_has_type
from dlt.common.storages import FilesystemConfiguration, fsspec_from_config


from dlt.destinations.insert_job_client import InsertValuesJobClient
from dlt.destinations.job_impl import FinalizedLoadJobWithFollowupJobs
from dlt.destinations.exceptions import LoadJobTerminalException
from dlt.destinations.impl.databricks.configuration import DatabricksClientConfiguration
from dlt.destinations.impl.databricks.sql_client import DatabricksSqlClient
from dlt.destinations.sql_jobs import SqlMergeFollowupJob
from dlt.destinations.job_impl import ReferenceFollowupJobRequest
from dlt.destinations.type_mapping import TypeMapper


AZURE_BLOB_STORAGE_PROTOCOLS = ["az", "abfss", "abfs"]


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
        if precision <= 64:
            return "BIGINT"
        raise TerminalValueError(
            f"bigint with {precision} bits precision cannot be mapped into databricks integer type"
        )

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
        staging_credentials = self._staging_config.credentials
        # extract and prepare some vars
        bucket_path = orig_bucket_path = (
            ReferenceFollowupJobRequest.resolve_reference(self._file_path)
            if ReferenceFollowupJobRequest.is_reference_job(self._file_path)
            else ""
        )
        file_name = (
            FileStorage.get_file_name_from_file_path(bucket_path)
            if bucket_path
            else self._file_name
        )
        from_clause = ""
        credentials_clause = ""
        format_options_clause = ""

        if bucket_path:
            bucket_url = urlparse(bucket_path)
            bucket_scheme = bucket_url.scheme

            if bucket_scheme not in AZURE_BLOB_STORAGE_PROTOCOLS + ["s3"]:
                raise LoadJobTerminalException(
                    self._file_path,
                    f"Databricks cannot load data from staging bucket {bucket_path}. Only s3 and"
                    " azure buckets are supported",
                )

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
                    ))
                    """
                elif bucket_scheme in AZURE_BLOB_STORAGE_PROTOCOLS:
                    assert isinstance(staging_credentials, AzureCredentialsWithoutDefaults)
                    # Explicit azure credentials are needed to load from bucket without a named stage
                    credentials_clause = f"""WITH(CREDENTIAL(AZURE_SAS_TOKEN='{staging_credentials.azure_storage_sas_token}'))"""
                    bucket_path = self.ensure_databricks_abfss_url(
                        bucket_path, staging_credentials.azure_storage_account_name
                    )

            if bucket_scheme in AZURE_BLOB_STORAGE_PROTOCOLS:
                assert isinstance(staging_credentials, AzureCredentialsWithoutDefaults)
                bucket_path = self.ensure_databricks_abfss_url(
                    bucket_path, staging_credentials.azure_storage_account_name
                )

            # always add FROM clause
            from_clause = f"FROM '{bucket_path}'"
        else:
            raise LoadJobTerminalException(
                self._file_path,
                "Cannot load from local file. Databricks does not support loading from local files."
                " Configure staging with an s3 or azure storage bucket.",
            )

        # decide on source format, stage_file_path will either be a local file or a bucket path
        if file_name.endswith(".parquet"):
            source_format = "PARQUET"  # Only parquet is supported
        elif file_name.endswith(".jsonl"):
            if not config.get("data_writer.disable_compression"):
                raise LoadJobTerminalException(
                    self._file_path,
                    "Databricks loader does not support gzip compressed JSON files. Please disable"
                    " compression in the data writer configuration:"
                    " https://dlthub.com/docs/reference/performance#disabling-and-enabling-file-compression",
                )
            if table_schema_has_type(self._load_table, "decimal"):
                raise LoadJobTerminalException(
                    self._file_path,
                    "Databricks loader cannot load DECIMAL type columns from json files. Switch to"
                    " parquet format to load decimals.",
                )
            if table_schema_has_type(self._load_table, "binary"):
                raise LoadJobTerminalException(
                    self._file_path,
                    "Databricks loader cannot load BINARY type columns from json files. Switch to"
                    " parquet format to load byte values.",
                )
            if table_schema_has_type(self._load_table, "complex"):
                raise LoadJobTerminalException(
                    self._file_path,
                    "Databricks loader cannot load complex columns (lists and dicts) from json"
                    " files. Switch to parquet format to load complex types.",
                )
            if table_schema_has_type(self._load_table, "date"):
                raise LoadJobTerminalException(
                    self._file_path,
                    "Databricks loader cannot load DATE type columns from json files. Switch to"
                    " parquet format to load dates.",
                )

            source_format = "JSON"
            format_options_clause = "FORMAT_OPTIONS('inferTimestamp'='true')"
            # Databricks fails when trying to load empty json files, so we have to check the file size
            fs, _ = fsspec_from_config(self._staging_config)
            file_size = fs.size(orig_bucket_path)
            if file_size == 0:  # Empty file, do nothing
                return

        statement = f"""COPY INTO {qualified_table_name}
            {from_clause}
            {credentials_clause}
            FILEFORMAT = {source_format}
            {format_options_clause}
            """
        self._sql_client.execute_sql(statement)

    @staticmethod
    def ensure_databricks_abfss_url(
        bucket_path: str, azure_storage_account_name: str = None
    ) -> str:
        bucket_url = urlparse(bucket_path)
        # Converts an az://<container_name>/<path> to abfss://<container_name>@<storage_account_name>.dfs.core.windows.net/<path>
        if bucket_url.username:
            # has the right form, ensure abfss schema
            return urlunparse(bucket_url._replace(scheme="abfss"))

        if not azure_storage_account_name:
            raise TerminalValueError(
                f"Could not convert azure blob storage url {bucket_path} into form required by"
                " Databricks"
                " (abfss://<container_name>@<storage_account_name>.dfs.core.windows.net/<path>)"
                " because storage account name is not known. Please use Databricks abfss://"
                " canonical url as bucket_url in staging credentials"
            )
        # as required by databricks
        _path = bucket_url.path
        return urlunparse(
            bucket_url._replace(
                scheme="abfss",
                netloc=f"{bucket_url.netloc}@{azure_storage_account_name}.dfs.core.windows.net",
                path=_path,
            )
        )


class DatabricksMergeJob(SqlMergeFollowupJob):
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
        self.sql_client: DatabricksSqlClient = sql_client  # type: ignore[assignment]
        self.type_mapper = DatabricksTypeMapper(self.capabilities)

    def create_load_job(
        self, table: TTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        job = super().create_load_job(table, file_path, load_id, restore)

        if not job:
            job = DatabricksLoadJob(
                file_path,
                staging_config=cast(FilesystemConfiguration, self.config.staging_config),
            )
        return job

    def _create_merge_followup_jobs(
        self, table_chain: Sequence[TTableSchema]
    ) -> List[FollowupJobRequest]:
        return [DatabricksMergeJob.from_table_chain(table_chain, self.sql_client)]

    def _make_add_column_sql(
        self, new_columns: Sequence[TColumnSchema], table_format: TTableFormat = None
    ) -> List[str]:
        # Override because databricks requires multiple columns in a single ADD COLUMN clause
        return ["ADD COLUMN\n" + ",\n".join(self._get_column_def_sql(c) for c in new_columns)]

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
        return self.type_mapper.from_db_type(bq_t, precision, scale)

    def _get_column_def_sql(self, c: TColumnSchema, table_format: TTableFormat = None) -> str:
        name = self.sql_client.escape_column_name(c["name"])
        return (
            f"{name} {self.type_mapper.to_db_type(c)} {self._gen_not_null(c.get('nullable', True))}"
        )

    def _get_storage_table_query_columns(self) -> List[str]:
        fields = super()._get_storage_table_query_columns()
        fields[2] = (  # Override because this is the only way to get data type with precision
            "full_data_type"
        )
        return fields

    def should_truncate_table_before_load_on_staging_destination(self, table: TTableSchema) -> bool:
        return self.config.truncate_tables_on_staging_destination_before_load
