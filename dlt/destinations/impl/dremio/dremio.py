from typing import ClassVar, Optional, Sequence, Tuple, List, Type
from urllib.parse import urlparse, urlunparse

from dlt.common.configuration.specs import (
    AwsCredentialsWithoutDefaults,
    AzureCredentialsWithoutDefaults,
)
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import (
    FollowupJob,
    TLoadJobState,
    LoadJob,
    CredentialsConfiguration,
    SupportsStagingDestination,
    NewLoadJob,
)
from dlt.common.schema import TColumnSchema, Schema, TTableSchemaColumns
from dlt.common.schema.typing import TTableSchema, TColumnType, TTableFormat, TColumnSchemaBase
from dlt.common.storages.file_storage import FileStorage
from dlt.common.utils import uniq_id
from dlt.destinations.exceptions import LoadJobTerminalException
from dlt.destinations.impl.dremio import capabilities
from dlt.destinations.impl.dremio.configuration import DremioClientConfiguration
from dlt.destinations.impl.dremio.sql_client import DremioSqlClient
from dlt.destinations.job_client_impl import SqlJobClientWithStaging
from dlt.destinations.job_impl import EmptyLoadJob
from dlt.destinations.job_impl import NewReferenceJob
from dlt.destinations.sql_jobs import SqlMergeJob
from dlt.destinations.type_mapping import TypeMapper


class DremioTypeMapper(TypeMapper):
    BIGINT_PRECISION = 19
    sct_to_unbound_dbt = {
        "complex": "VARCHAR",
        "text": "VARCHAR",
        "double": "DOUBLE",
        "bool": "BOOLEAN",
        "date": "DATE",
        "timestamp": "TIMESTAMP",
        "bigint": "BIGINT",
        "binary": "VARBINARY",
        "time": "TIME",
    }

    sct_to_dbt = {
        "decimal": "DECIMAL(%i,%i)",
        "wei": "DECIMAL(%i,%i)",
    }

    dbt_to_sct = {
        "VARCHAR": "text",
        "DOUBLE": "double",
        "FLOAT": "double",
        "BOOLEAN": "bool",
        "DATE": "date",
        "TIMESTAMP": "timestamp",
        "VARBINARY": "binary",
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


class DremioLoadJob(LoadJob, FollowupJob):
    def __init__(
        self,
        file_path: str,
        table_name: str,
        client: DremioSqlClient,
        stage_name: Optional[str] = None,
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

        if not bucket_path:
            raise RuntimeError("Could not resolve bucket path.")

        file_name = (
            FileStorage.get_file_name_from_file_path(bucket_path) if bucket_path else file_name
        )

        bucket_url = urlparse(bucket_path)
        bucket_scheme = bucket_url.scheme
        # referencing an external s3/azure stage does not require explicit AWS credentials
        if bucket_scheme in ["s3", "az", "abfs"] and stage_name:
            from_clause = f"FROM '@{stage_name}/{bucket_url.hostname}'"
            files_clause = f"FILES ('{bucket_url.path.lstrip('/')}')"
        # referencing an staged files via a bucket URL requires explicit AWS credentials
        elif (
            bucket_scheme == "s3"
            and staging_credentials
            and isinstance(staging_credentials, AwsCredentialsWithoutDefaults)
        ):
            from_clause = f"FROM '{bucket_path}'"
            files_clause = ""
        elif (
            bucket_scheme in ["az", "abfs"]
            and staging_credentials
            and isinstance(staging_credentials, AzureCredentialsWithoutDefaults)
        ):
            # Converts an az://<container_name>/<path> to azure://<storage_account_name>.blob.core.windows.net/<container_name>/<path>
            # as required by snowflake
            _path = "/" + bucket_url.netloc + bucket_url.path
            bucket_path = urlunparse(
                bucket_url._replace(
                    scheme="azure",
                    netloc=(
                        f"{staging_credentials.azure_storage_account_name}.blob.core.windows.net"
                    ),
                    path=_path,
                )
            )
            from_clause = f"FROM '{bucket_path}'"
            files_clause = ""
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
            files_clause = f"FILES ('{urlparse(bucket_path).path.lstrip('/')}')"

        source_format = file_name.split(".")[-1]

        client.execute_sql(f"""COPY INTO {qualified_table_name}
            {from_clause}
            {files_clause}
            FILE_FORMAT '{source_format}'
            """)

    def state(self) -> TLoadJobState:
        return "completed"

    def exception(self) -> str:
        raise NotImplementedError()


class DremioClient(SqlJobClientWithStaging, SupportsStagingDestination):
    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, schema: Schema, config: DremioClientConfiguration) -> None:
        sql_client = DremioSqlClient(config.normalize_dataset_name(schema), config.credentials)
        super().__init__(schema, config, sql_client)
        self.config: DremioClientConfiguration = config
        self.sql_client: DremioSqlClient = sql_client  # type: ignore
        self.type_mapper = DremioTypeMapper(self.capabilities)

    def start_file_load(self, table: TTableSchema, file_path: str, load_id: str) -> LoadJob:
        job = super().start_file_load(table, file_path, load_id)

        if not job:
            job = DremioLoadJob(
                file_path=file_path,
                table_name=table["name"],
                client=self.sql_client,
                stage_name=self.config.staging_data_source,
                staging_credentials=(
                    self.config.staging_config.credentials if self.config.staging_config else None
                ),
            )
        return job

    def restore_file_load(self, file_path: str) -> LoadJob:
        return EmptyLoadJob.from_file_path(file_path, "completed")

    def _get_table_update_sql(
        self,
        table_name: str,
        new_columns: Sequence[TColumnSchema],
        generate_alter: bool,
        separate_alters: bool = False,
    ) -> List[str]:
        sql = super()._get_table_update_sql(table_name, new_columns, generate_alter)

        if not generate_alter:
            partition_list = [
                self.capabilities.escape_identifier(c["name"])
                for c in new_columns
                if c.get("partition")
            ]
            if partition_list:
                sql[0] += "\nPARTITION BY (" + ",".join(partition_list) + ")"

            sort_list = [
                self.capabilities.escape_identifier(c["name"]) for c in new_columns if c.get("sort")
            ]
            if sort_list:
                sql[0] += "\nLOCALSORT BY (" + ",".join(sort_list) + ")"

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
        def _null_to_bool(v: str) -> bool:
            if v == "NO":
                return False
            elif v == "YES":
                return True
            raise ValueError(v)

        fields = self._get_storage_table_query_columns()
        table_schema = self.sql_client.fully_qualified_dataset_name(escape=False)
        if self.sql_client.credentials.flatten:
            table_name = f"{self.sql_client.dataset_name}__{table_name}"
        db_params = (table_schema, table_name)
        query = f"""
SELECT {",".join(fields)}
    FROM INFORMATION_SCHEMA.COLUMNS
WHERE
    table_catalog = 'DREMIO' AND table_schema = %s AND table_name = %s ORDER BY ordinal_position;
"""
        rows = self.sql_client.execute_sql(query, *db_params)

        # if no rows we assume that table does not exist
        schema_table: TTableSchemaColumns = {}
        if len(rows) == 0:
            return False, schema_table
        for c in rows:
            numeric_precision = (
                c[3] if self.capabilities.schema_supports_numeric_precision else None
            )
            numeric_scale = c[4] if self.capabilities.schema_supports_numeric_precision else None
            schema_c: TColumnSchemaBase = {
                "name": c[0],
                "nullable": _null_to_bool(c[2]),
                **self._from_db_type(c[1], numeric_precision, numeric_scale),
            }
            schema_table[c[0]] = schema_c  # type: ignore
        return True, schema_table

    def _create_merge_followup_jobs(self, table_chain: Sequence[TTableSchema]) -> List[NewLoadJob]:
        def _new_temp_table_name(name_prefix: str) -> str:
            return self.sql_client.make_qualified_table_name(f"_temp_{name_prefix}_{uniq_id()}")

        class DremioMergeJob(SqlMergeJob):
            @classmethod
            def _new_temp_table_name(cls, name_prefix: str) -> str:
                return _new_temp_table_name(name_prefix)

            @classmethod
            def _to_temp_table(cls, select_sql: str, temp_table_name: str) -> str:
                return f"CREATE TABLE {temp_table_name} AS {select_sql};"

        DremioMergeJob._new_temp_table_name = _new_temp_table_name

        return [DremioMergeJob.from_table_chain(table_chain, self.sql_client)]
