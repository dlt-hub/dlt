from copy import deepcopy
from textwrap import dedent
from typing import Any, Literal, Optional, List, Sequence, cast
from urllib.parse import urlparse

import clickhouse_connect

from dlt.common.configuration.specs import (
    CredentialsConfiguration,
    AzureCredentialsWithoutDefaults,
    AwsCredentialsWithoutDefaults,
)
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.client import (
    PreparedTableSchema,
    SupportsStagingDestination,
    HasFollowupJobs,
    RunnableLoadJob,
    FollowupJobRequest,
    LoadJob,
)
from dlt.common.schema import Schema, TColumnSchema
from dlt.common.schema.exceptions import SchemaCorruptedException
from dlt.common.schema.typing import TColumnType
from dlt.common.schema.utils import get_columns_names_with_prop, is_nullable_column
from dlt.common.storages import FileStorage
from dlt.common.storages.configuration import FilesystemConfiguration, ensure_canonical_az_url
from dlt.common.storages.fsspec_filesystem import AZURE_BLOB_STORAGE_PROTOCOLS
from dlt.destinations.exceptions import LoadJobTerminalException
from dlt.destinations.impl.clickhouse.configuration import (
    ClickHouseClientConfiguration,
)
from dlt.destinations.impl.clickhouse.sql_client import ClickHouseSqlClient
from dlt.destinations.impl.clickhouse.typing import (
    CODEC_HINT,
    HINT_TO_CLICKHOUSE_ATTR,
    PARTITION_HINT,
    SETTINGS_HINT,
    SORT_HINT,
    TABLE_ENGINE_TYPE_TO_CLICKHOUSE_ATTR,
    TMergeTreeSettings,
    TMergeTreeSettingsValue,
)
from dlt.destinations.impl.clickhouse.typing import (
    TTableEngineType,
    TABLE_ENGINE_TYPE_HINT,
    FILE_FORMAT_TO_TABLE_FUNCTION_MAPPING,
    SUPPORTED_FILE_FORMATS,
)
from dlt.destinations.impl.clickhouse.utils import (
    convert_storage_to_http_scheme,
)
from dlt.destinations.job_client_impl import (
    SqlJobClientBase,
    SqlJobClientWithStagingDataset,
)
from dlt.destinations.job_impl import ReferenceFollowupJobRequest
from dlt.destinations.sql_client import SqlClientBase
from dlt.destinations.sql_jobs import SqlMergeFollowupJob
from dlt.destinations.utils import get_deterministic_temp_table_name
from dlt.destinations.path_utils import get_file_format_and_compression


class ClickHouseLoadJob(RunnableLoadJob, HasFollowupJobs):
    def __init__(
        self,
        file_path: str,
        config: ClickHouseClientConfiguration,
        staging_credentials: Optional[CredentialsConfiguration] = None,
    ) -> None:
        super().__init__(file_path)
        self._job_client: "ClickHouseClient" = None
        self._staging_credentials = staging_credentials
        self._config = config

    def run(self) -> None:
        client = self._job_client.sql_client

        bucket_path = None
        file_name = self._file_name

        if ReferenceFollowupJobRequest.is_reference_job(self._file_path):
            bucket_path = ReferenceFollowupJobRequest.resolve_reference(self._file_path)
            file_name = FileStorage.get_file_name_from_file_path(bucket_path)
            bucket_url = urlparse(bucket_path)
            bucket_scheme = bucket_url.scheme

        compression = "auto"

        file_format, is_compressed = get_file_format_and_compression(file_name)
        clickhouse_format: str = FILE_FORMAT_TO_TABLE_FUNCTION_MAPPING[
            cast(SUPPORTED_FILE_FORMATS, file_format)
        ]

        if file_format == "jsonl":
            # Auto does not work for jsonl. So we set it to 'none' if not compressed
            compression = "gz" if is_compressed else "none"

        # Don't use the DBAPI driver for local files.
        if not bucket_path or bucket_scheme == "file":
            file_path = (
                self._file_path
                if not bucket_path
                else FilesystemConfiguration.make_local_path(bucket_path)
            )
            try:
                client.insert_file(file_path, self.load_table_name, clickhouse_format, compression)
            except clickhouse_connect.driver.exceptions.Error as e:
                raise LoadJobTerminalException(
                    self._file_path,
                    f"ClickHouse connection failed due to `{e}`.",
                ) from e
            return

        qualified_table_name = client.make_qualified_table_name(self.load_table_name)

        if bucket_scheme in ("s3", "gs", "gcs"):
            if not isinstance(self._staging_credentials, AwsCredentialsWithoutDefaults):
                raise LoadJobTerminalException(
                    self._file_path,
                    dedent(
                        """
                        Google Cloud Storage buckets must be configured using the S3 compatible access pattern.
                        Please provide the necessary S3 credentials (access key ID and secret access key), to access the GCS bucket through the S3 API.
                        Refer to https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem#using-s3-compatible-storage.
                    """,
                    ).strip(),
                )

            bucket_http_url = convert_storage_to_http_scheme(
                bucket_url,
                endpoint=self._staging_credentials.endpoint_url,
                use_https=self._config.staging_use_https,
            )
            access_key_id = self._staging_credentials.aws_access_key_id
            secret_access_key = self._staging_credentials.aws_secret_access_key
            auth = "NOSIGN"
            if access_key_id and secret_access_key:
                auth = f"'{access_key_id}','{secret_access_key}'"

            table_function = (
                f"s3('{bucket_http_url}',{auth},'{clickhouse_format}','auto','{compression}')"
            )

        elif bucket_scheme in AZURE_BLOB_STORAGE_PROTOCOLS:
            if not isinstance(self._staging_credentials, AzureCredentialsWithoutDefaults):
                raise LoadJobTerminalException(
                    self._file_path,
                    "Unsigned Azure Blob Storage access from ClickHouse isn't supported as yet.",
                )

            # Authenticated access.
            account_name = self._staging_credentials.azure_storage_account_name
            account_host = (
                self._staging_credentials.azure_account_host
                or f"{account_name}.blob.core.windows.net"
            )
            storage_account_url = ensure_canonical_az_url("", "https", account_name, account_host)
            account_key = self._staging_credentials.azure_storage_account_key

            # build table func
            table_function = f"azureBlobStorage('{storage_account_url}','{bucket_url.netloc}','{bucket_url.path}','{account_name}','{account_key}','{clickhouse_format}','{compression}')"
        else:
            raise LoadJobTerminalException(
                self._file_path,
                f"ClickHouse loader does not support `{bucket_scheme}` filesystem.",
            )

        statement = f"INSERT INTO {qualified_table_name} SELECT * FROM {table_function}"
        with client.begin_transaction():
            client.execute_sql(statement)


class ClickHouseMergeJob(SqlMergeFollowupJob):
    @classmethod
    def _new_temp_table_name(cls, table_name: str, op: str, sql_client: SqlClientBase[Any]) -> str:
        # reproducible name so we know which table to drop
        with sql_client.with_staging_dataset():
            return sql_client.make_qualified_table_name(
                cls._shorten_table_name(
                    get_deterministic_temp_table_name(table_name, op), sql_client
                )
            )

    @classmethod
    def _to_temp_table(cls, select_sql: str, temp_table_name: str, unique_column: str) -> str:
        return (
            f"DROP TABLE IF EXISTS {temp_table_name} SYNC; CREATE TABLE {temp_table_name} ENGINE ="
            f" MergeTree PRIMARY KEY {unique_column} AS {select_sql}"
        )

    @classmethod
    def gen_key_table_clauses(
        cls,
        root_table_name: str,
        staging_root_table_name: str,
        key_clauses: Sequence[str],
        for_delete: bool,
    ) -> List[str]:
        join_conditions = " OR ".join([c.format(d="d", s="s") for c in key_clauses])
        return [
            f"FROM {root_table_name} AS d JOIN {staging_root_table_name} AS s ON {join_conditions}"
        ]

    @classmethod
    def gen_update_table_prefix(cls, table_name: str) -> str:
        return f"ALTER TABLE {table_name} UPDATE"

    @classmethod
    def requires_temp_table_for_delete(cls) -> bool:
        return True


class ClickHouseClient(SqlJobClientWithStagingDataset, SupportsStagingDestination):
    def __init__(
        self,
        schema: Schema,
        config: ClickHouseClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        dataset_name, staging_dataset_name = SqlJobClientWithStagingDataset.create_dataset_names(
            schema, config
        )
        self.sql_client: ClickHouseSqlClient = ClickHouseSqlClient(
            dataset_name,
            staging_dataset_name,
            list(schema.tables.keys()),
            config.credentials,
            capabilities,
            config,
        )
        super().__init__(schema, config, self.sql_client)
        self.config: ClickHouseClientConfiguration = config
        self.active_hints = deepcopy(HINT_TO_CLICKHOUSE_ATTR)
        self.type_mapper = self.capabilities.get_type_mapper()

    def prepare_load_table(self, table_name: str) -> Optional[PreparedTableSchema]:
        table = super().prepare_load_table(table_name)

        if SORT_HINT not in table:
            table[SORT_HINT] = get_columns_names_with_prop(table, "sort")  # type: ignore[typeddict-unknown-key]

        if PARTITION_HINT not in table:
            table[PARTITION_HINT] = get_columns_names_with_prop(table, "partition")  # type: ignore[typeddict-unknown-key]

        return table

    def _create_merge_followup_jobs(
        self, table_chain: Sequence[PreparedTableSchema]
    ) -> List[FollowupJobRequest]:
        return [ClickHouseMergeJob.from_table_chain(table_chain, self.sql_client)]

    def _get_column_def_sql(self, c: TColumnSchema, table: PreparedTableSchema = None) -> str:
        # Build column definition.
        # The primary key and sort order definition is defined outside column specification.
        hints_ = " ".join(
            self.active_hints.get(hint)
            for hint in self.active_hints.keys()
            if c.get(cast(str, hint), False) is True and hint not in ("primary_key", "sort")
        )

        if codec := c.get(CODEC_HINT):
            hints_ += f"CODEC({codec}) "

        # Alter table statements only accept `Nullable` modifiers.
        # JSON type isn't nullable in ClickHouse.
        type_with_nullability_modifier = (
            f"Nullable({self.type_mapper.to_destination_type(c,table)})"
            if is_nullable_column(c)
            else self.type_mapper.to_destination_type(c, table)
        )

        return (
            f"{self.sql_client.escape_column_name(c['name'])} {type_with_nullability_modifier} {hints_}"
            .strip()
        )

    def create_load_job(
        self, table: PreparedTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        return super().create_load_job(table, file_path, load_id, restore) or ClickHouseLoadJob(
            file_path,
            config=self.config,
            staging_credentials=(
                self.config.staging_config.credentials if self.config.staging_config else None
            ),
        )

    def _get_key(
        self,
        table_schema: PreparedTableSchema,
        key_type: Literal["sort", "partition"],
    ) -> Optional[str]:
        """Returns sort or partition key based on hint."""

        hint = table_schema.get(SORT_HINT if key_type == "sort" else PARTITION_HINT)
        if not hint:
            return None

        assert isinstance(hint, (str, list, tuple))

        if isinstance(hint, str):
            # it's a SQL expression; return as is
            return hint
        elif isinstance(hint, (list, tuple)):
            # it's a sequence of column names; generate expression
            norm_hint_columns = [self.schema.naming.normalize_identifier(col) for col in hint]
            return "(" + ", ".join(norm_hint_columns) + ")"

    def _get_settings_clause(self, settings_hint: TMergeTreeSettings) -> str:
        def to_clickhouse_literal(val: TMergeTreeSettingsValue) -> str:
            if isinstance(val, str):
                escaped_str = self.capabilities.escape_literal(val)
                return cast(str, escaped_str)
            elif isinstance(val, bool):
                return "true" if val else "false"
            return str(val)

        clauses = [f"{key} = {to_clickhouse_literal(val)}" for key, val in settings_hint.items()]

        return ", ".join(clauses)

    def _get_table_update_sql(
        self, table_name: str, new_columns: Sequence[TColumnSchema], generate_alter: bool
    ) -> List[str]:
        table = self.prepare_load_table(table_name)
        sql = SqlJobClientBase._get_table_update_sql(self, table_name, new_columns, generate_alter)

        if generate_alter:
            return sql

        # Default to 'MergeTree' if the user didn't explicitly set a table engine hint.
        # Clickhouse Cloud will automatically pick `SharedMergeTree` for this option,
        # so it will work on both local and cloud instances of CH.
        table_type = cast(
            TTableEngineType,
            table.get(
                cast(str, TABLE_ENGINE_TYPE_HINT),
                self.config.table_engine_type,
            ),
        )
        sql[0] = f"{sql[0]}\nENGINE = {TABLE_ENGINE_TYPE_TO_CLICKHOUSE_ATTR.get(table_type)}"

        # PRIMARY KEY
        if primary_key_list := [
            self.sql_client.escape_column_name(c["name"])
            for c in new_columns
            if c.get("primary_key")
        ]:
            sql[0] += "\nPRIMARY KEY (" + ", ".join(primary_key_list) + ")"
        else:
            sql[0] += "\nPRIMARY KEY tuple()"

        # ORDER BY
        if sort_key := self._get_key(table, "sort"):
            sql[0] += f"\nORDER BY {sort_key}"

        # PARTITION BY
        if part_key := self._get_key(table, "partition"):
            sql[0] += f"\nPARTITION BY {part_key}"

        # SETTNGS
        if settings_hint := table.get(SETTINGS_HINT):
            settings_hint = cast(TMergeTreeSettings, settings_hint)
            settings_clause = self._get_settings_clause(settings_hint)
            sql[0] += f"\nSETTINGS {settings_clause}"

        return sql

    def _gen_not_null(self, v: bool) -> str:
        # ClickHouse fields are not nullable by default.
        # We use the `Nullable` modifier instead of NULL / NOT NULL modifiers to cater for ALTER statement.
        return ""

    def _from_db_type(
        self, ch_t: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        return self.type_mapper.from_destination_type(ch_t, precision, scale)

    def should_truncate_table_before_load_on_staging_destination(self, table_name: str) -> bool:
        return self.config.truncate_tables_on_staging_destination_before_load
