from copy import deepcopy
from typing import ClassVar, Optional, Dict, List, Sequence
from urllib.parse import urlparse, urlunparse

from dlt.common.configuration.specs import (
    CredentialsConfiguration,
    AwsCredentialsWithoutDefaults,
    AzureCredentialsWithoutDefaults,
)
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import (
    SupportsStagingDestination,
    TLoadJobState,
    FollowupJob,
    LoadJob,
)
from dlt.common.schema import Schema, TColumnSchema
from dlt.common.schema.typing import TTableFormat, TTableSchema, TColumnHint, TColumnType
from dlt.common.storages import FileStorage
from dlt.destinations.exceptions import LoadJobTerminalException
from dlt.destinations.impl.clickhouse import capabilities
from dlt.destinations.impl.clickhouse.clickhouse_adapter import (
    TTableEngineType,
    TABLE_ENGINE_TYPE_HINT,
)
from dlt.destinations.impl.clickhouse.configuration import (
    ClickhouseClientConfiguration,
)
from dlt.destinations.impl.clickhouse.sql_client import ClickhouseSqlClient
from dlt.destinations.job_client_impl import (
    SqlJobClientWithStaging,
    SqlJobClientBase,
)
from dlt.destinations.job_impl import NewReferenceJob, EmptyLoadJob
from dlt.destinations.type_mapping import TypeMapper


HINT_TO_CLICKHOUSE_ATTR: Dict[TColumnHint, str] = {
    "primary_key": "PRIMARY KEY",
}

TABLE_ENGINE_TYPE_TO_CLICKHOUSE_ATTR: Dict[TTableEngineType, str] = {
    "merge_tree": "MergeTree",
    "replicated_merge_tree": "ReplicatedMergeTree",
}


class ClickhouseTypeMapper(TypeMapper):
    sct_to_unbound_dbt = {
        "complex": "JSON",
        "text": "String",
        "double": "Float64",
        "bool": "Boolean",
        "date": "Date",
        "timestamp": "DateTime",
        "bigint": "Int64",
        "binary": "String",
        "wei": "Decimal",
    }

    sct_to_dbt = {
        "decimal": "Decimal(%i,%i)",
        "wei": "Decimal(%i,%i)",
        "timestamp": "DateTime(%i)",
    }

    dbt_to_sct = {
        "String": "text",
        "Float64": "double",
        "Boolean": "bool",
        "Date": "date",
        "DateTime": "timestamp",
        "Int64": "bigint",
        "JSON": "complex",
        "Decimal": "decimal",
    }

    def to_db_time_type(self, precision: Optional[int], table_format: TTableFormat = None) -> str:
        return "DateTime"

    def from_db_type(
        self, db_type: str, precision: Optional[int] = None, scale: Optional[int] = None
    ) -> TColumnType:
        if db_type == "Decimal" and (precision, scale) == self.capabilities.wei_precision:
            return dict(data_type="wei")
        return super().from_db_type(db_type, precision, scale)


class ClickhouseLoadJob(LoadJob, FollowupJob):
    def __init__(
        self,
        file_path: str,
        table_name: str,
        load_id: str,
        client: ClickhouseSqlClient,
        staging_credentials: Optional[CredentialsConfiguration] = None,
    ) -> None:
        file_name = FileStorage.get_file_name_from_file_path(file_path)
        super().__init__(file_name)

        qualified_table_name = client.make_qualified_table_name(table_name)

        bucket_path: str = (
            NewReferenceJob.resolve_reference(file_path)
            if NewReferenceJob.is_reference_job(file_path)
            else ""
        )
        file_name = (
            FileStorage.get_file_name_from_file_path(bucket_path) if bucket_path else file_name
        )
        credentials_clause = ""
        files_clause = ""

        if bucket_path:
            bucket_url = urlparse(bucket_path)
            bucket_scheme = bucket_url.scheme
            # Referencing an external s3/azure stage does not require explicit AWS credentials.
            if (
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
                # Converts an az://<container_name>/<path> to azure://<storage_account_name>.blob.core.windows.net/<container_name>/<path> as required by Clickhouse.
                _path = f"/{bucket_url.netloc}{bucket_url.path}"
                bucket_path = urlunparse(
                    bucket_url._replace(
                        scheme="azure",
                        netloc=f"{staging_credentials.azure_storage_account_name}.blob.core.windows.net",
                        path=_path,
                    )
                )
                from_clause = f"FROM '{bucket_path}'"
            else:
                # Ensure that gcs bucket path starts with gcs://; this is a requirement of Clickhouse.
                bucket_path = bucket_path.replace("gs://", "gcs://")
                from_clause = f"FROM @{stage_name}/"
                files_clause = f"FILES = ('{urlparse(bucket_path).path.lstrip('/')}')"
        else:
            # This means we have a local file.
            if not stage_name:
                # Use implicit table stage by default: "SCHEMA_NAME"."%TABLE_NAME".
                stage_name = client.make_qualified_table_name(f"%{table_name}")
            stage_file_path = f'@{stage_name}/"{load_id}"/{file_name}'
            from_clause = f"FROM {stage_file_path}"

        # Decide on source format, stage_file_path will either be a local file or a bucket path.
        source_format = "( TYPE = 'JSON', BINARY_FORMAT = 'BASE64' )"
        if file_name.endswith("parquet"):
            source_format = "(TYPE = 'PARQUET', BINARY_AS_TEXT = FALSE)"

        with client.begin_transaction():
            # PUT and COPY in one transaction if local file, otherwise only copy.
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

    def state(self) -> TLoadJobState:
        return "completed"

    def exception(self) -> str:
        raise NotImplementedError()


class ClickhouseClient(SqlJobClientWithStaging, SupportsStagingDestination):
    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(
        self,
        schema: Schema,
        config: ClickhouseClientConfiguration,
    ) -> None:
        # TODO: There are no schemas in Clickhouse. No point in having schemas, only dataset names and table names for example "dataset1_mytable".
        self.sql_client: ClickhouseSqlClient = ClickhouseSqlClient(
            config.normalize_dataset_name(schema), config.credentials
        )
        super().__init__(schema, config, self.sql_client)
        self.config: ClickhouseClientConfiguration = config
        self.active_hints = deepcopy(HINT_TO_CLICKHOUSE_ATTR) if self.config.create_indexes else {}
        self.type_mapper = ClickhouseTypeMapper(self.capabilities)

    def start_file_load(self, table: TTableSchema, file_path: str, load_id: str) -> LoadJob:
        return super().start_file_load(table, file_path, load_id) or ClickhouseLoadJob(
            file_path,
            table["name"],
            load_id,
            self.sql_client,
            staging_credentials=(
                self.config.staging_config.credentials if self.config.staging_config else None
            ),
        )

    def _get_table_update_sql(
        self, table_name: str, new_columns: Sequence[TColumnSchema], generate_alter: bool
    ) -> List[str]:
        table: TTableSchema = self.prepare_load_table(table_name, self.in_staging_mode)
        sql = SqlJobClientBase._get_table_update_sql(self, table_name, new_columns, generate_alter)

        if generate_alter:
            return sql

        # TODO: Remove `unique` and `primary_key` default implementations.
        if primary_key_list := [
            self.capabilities.escape_identifier(c["name"])
            for c in new_columns
            if c.get("primary_key")
        ]:
            sql[0] += "\nPRIMARY KEY (" + ", ".join(primary_key_list) + ")"
        else:
            sql[0] += "\nPRIMARY KEY tuple()"

        # Default to 'ReplicatedMergeTree' if user didn't explicitly set a table engine hint.
        # 'ReplicatedMergeTree' is the only supported engine for Clickhouse Cloud.
        sql[0] = f"{sql[0]}\nENGINE = {table.get(TABLE_ENGINE_TYPE_HINT, 'replicated_merge_tree')}"

        return sql

    def _get_column_def_sql(self, c: TColumnSchema, table_format: TTableFormat = None) -> str:
        # The primary key definition is defined outside column specification.
        hints_str = " ".join(
            self.active_hints.get(hint, "")
            for hint in self.active_hints.keys()
            if c.get(hint, False) is True and hint != "primary_key"
        )
        return (
            f"{self.capabilities.escape_identifier(c['name'])} "
            f"{self.type_mapper.to_db_type(c)} "
            f"{hints_str} "
            f"{self._gen_not_null(c.get('nullable', True))}"
        )

    # Clickhouse fields are not nullable by default.
    @staticmethod
    def _gen_not_null(v: bool) -> str:
        return "NULL" if v else "NOT NULL"

    def _from_db_type(
        self, ch_t: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        return self.type_mapper.from_db_type(ch_t, precision, scale)

    def restore_file_load(self, file_path: str) -> LoadJob:
        return EmptyLoadJob.from_file_path(file_path, "completed")
