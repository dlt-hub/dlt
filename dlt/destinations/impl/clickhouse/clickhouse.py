import os
import re
from copy import deepcopy
from typing import ClassVar, Optional, Dict, List, Sequence, cast, Tuple
from urllib.parse import urlparse

import clickhouse_connect
from clickhouse_connect.driver.tools import insert_file

import dlt
from dlt import config
from dlt.common.configuration.specs import (
    CredentialsConfiguration,
    AzureCredentialsWithoutDefaults,
    GcpCredentials,
    AwsCredentialsWithoutDefaults,
)
from dlt.destinations.exceptions import DestinationTransientException
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import (
    SupportsStagingDestination,
    TLoadJobState,
    FollowupJob,
    LoadJob,
    NewLoadJob,
)
from dlt.common.schema import Schema, TColumnSchema
from dlt.common.schema.typing import (
    TTableFormat,
    TTableSchema,
    TColumnHint,
    TColumnType,
    TTableSchemaColumns,
    TColumnSchemaBase,
)
from dlt.common.storages import FileStorage
from dlt.destinations.exceptions import LoadJobTerminalException
from dlt.destinations.impl.clickhouse import capabilities
from dlt.destinations.impl.clickhouse.clickhouse_adapter import (
    TTableEngineType,
    TABLE_ENGINE_TYPE_HINT,
)
from dlt.destinations.impl.clickhouse.configuration import (
    ClickHouseClientConfiguration,
)
from dlt.destinations.impl.clickhouse.sql_client import ClickHouseSqlClient
from dlt.destinations.impl.clickhouse.utils import (
    convert_storage_to_http_scheme,
    FILE_FORMAT_TO_TABLE_FUNCTION_MAPPING,
    SUPPORTED_FILE_FORMATS,
)
from dlt.destinations.job_client_impl import (
    SqlJobClientBase,
    SqlJobClientWithStaging,
)
from dlt.destinations.job_impl import NewReferenceJob, EmptyLoadJob
from dlt.destinations.sql_jobs import SqlMergeJob
from dlt.destinations.type_mapping import TypeMapper


HINT_TO_CLICKHOUSE_ATTR: Dict[TColumnHint, str] = {
    "primary_key": "PRIMARY KEY",
    "unique": "",  # No unique constraints available in ClickHouse.
    "foreign_key": "",  # No foreign key constraints support in ClickHouse.
}

TABLE_ENGINE_TYPE_TO_CLICKHOUSE_ATTR: Dict[TTableEngineType, str] = {
    "merge_tree": "MergeTree",
    "replicated_merge_tree": "ReplicatedMergeTree",
}


class ClickHouseTypeMapper(TypeMapper):
    sct_to_unbound_dbt = {
        "complex": "String",
        "text": "String",
        "double": "Float64",
        "bool": "Boolean",
        "date": "Date",
        "timestamp": "DateTime64(6,'UTC')",
        "time": "String",
        "bigint": "Int64",
        "binary": "String",
        "wei": "Decimal",
    }

    sct_to_dbt = {
        "decimal": "Decimal(%i,%i)",
        "wei": "Decimal(%i,%i)",
        "timestamp": "DateTime64(%i,'UTC')",
    }

    dbt_to_sct = {
        "String": "text",
        "Float64": "double",
        "Bool": "bool",
        "Date": "date",
        "DateTime": "timestamp",
        "DateTime64": "timestamp",
        "Time": "timestamp",
        "Int64": "bigint",
        "Object('json')": "complex",
        "Decimal": "decimal",
    }

    def from_db_type(
        self, db_type: str, precision: Optional[int] = None, scale: Optional[int] = None
    ) -> TColumnType:
        # Remove "Nullable" wrapper.
        db_type = re.sub(r"^Nullable\((?P<type>.+)\)$", r"\g<type>", db_type)

        # Remove timezone details.
        if db_type == "DateTime('UTC')":
            db_type = "DateTime"
        if datetime_match := re.match(
            r"DateTime64(?:\((?P<precision>\d+)(?:,?\s*'(?P<timezone>UTC)')?\))?", db_type
        ):
            if datetime_match["precision"]:
                precision = int(datetime_match["precision"])
            else:
                precision = None
            db_type = "DateTime64"

        # Extract precision and scale, parameters and remove from string.
        if decimal_match := re.match(
            r"Decimal\((?P<precision>\d+)\s*(?:,\s*(?P<scale>\d+))?\)", db_type
        ):
            precision, scale = decimal_match.groups()  # type: ignore[assignment]
            precision = int(precision)
            scale = int(scale) if scale else 0
            db_type = "Decimal"

        if db_type == "Decimal" and (precision, scale) == self.capabilities.wei_precision:
            return dict(data_type="wei")

        return super().from_db_type(db_type, precision, scale)


class ClickHouseLoadJob(LoadJob, FollowupJob):
    def __init__(
        self,
        file_path: str,
        table_name: str,
        client: ClickHouseSqlClient,
        staging_credentials: Optional[CredentialsConfiguration] = None,
    ) -> None:
        file_name = FileStorage.get_file_name_from_file_path(file_path)
        super().__init__(file_name)

        qualified_table_name = client.make_qualified_table_name(table_name)
        bucket_path = None

        if NewReferenceJob.is_reference_job(file_path):
            bucket_path = NewReferenceJob.resolve_reference(file_path)
            file_name = FileStorage.get_file_name_from_file_path(bucket_path)
            bucket_url = urlparse(bucket_path)
            bucket_scheme = bucket_url.scheme

        ext = cast(SUPPORTED_FILE_FORMATS, os.path.splitext(file_name)[1][1:].lower())
        clickhouse_format: str = FILE_FORMAT_TO_TABLE_FUNCTION_MAPPING[ext]

        compression = "auto"

        # Don't use dbapi driver for local files.
        if not bucket_path:
            # Local filesystem.
            if ext == "jsonl":
                compression = "gz" if FileStorage.is_gzipped(file_path) else "none"
            try:
                with clickhouse_connect.create_client(
                    host=client.credentials.host,
                    port=client.credentials.http_port,
                    database=client.credentials.database,
                    user_name=client.credentials.username,
                    password=client.credentials.password,
                    secure=bool(client.credentials.secure),
                ) as clickhouse_connect_client:
                    insert_file(
                        clickhouse_connect_client,
                        qualified_table_name,
                        file_path,
                        fmt=clickhouse_format,
                        settings={
                            "allow_experimental_lightweight_delete": 1,
                            # "allow_experimental_object_type": 1,
                            "enable_http_compression": 1,
                        },
                        compression=compression,
                    )
            except clickhouse_connect.driver.exceptions.Error as e:
                raise LoadJobTerminalException(
                    file_path,
                    f"ClickHouse connection failed due to {e}.",
                ) from e
            return

        # Auto does not work for jsonl, get info from config for buckets
        # NOTE: we should not really be accessing the config this way, but for
        # now it is ok...
        if ext == "jsonl":
            compression = "none" if config.get("data_writer.disable_compression") else "gz"

        if bucket_scheme in ("s3", "gs", "gcs"):
            # get auth and bucket url
            bucket_http_url = convert_storage_to_http_scheme(bucket_url)
            access_key_id: str = None
            secret_access_key: str = None
            if isinstance(staging_credentials, AwsCredentialsWithoutDefaults):
                access_key_id = staging_credentials.aws_access_key_id
                secret_access_key = staging_credentials.aws_secret_access_key
            elif isinstance(staging_credentials, GcpCredentials):
                access_key_id = client.credentials.gcp_access_key_id
                secret_access_key = client.credentials.gcp_secret_access_key
                if not access_key_id or not secret_access_key:
                    raise DestinationTransientException(
                        "You have tried loading from gcs with clickhouse. Please provide valid"
                        " 'gcp_access_key_id' and 'gcp_secret_access_key' to connect to gcs as"
                        " outlined in the dlthub docs."
                    )

            auth = "NOSIGN"
            if access_key_id and secret_access_key:
                auth = f"'{access_key_id}','{secret_access_key}'"

            table_function = (
                f"s3('{bucket_http_url}',{auth},'{clickhouse_format}','auto','{compression}')"
            )

        elif bucket_scheme in ("az", "abfs"):
            if not isinstance(staging_credentials, AzureCredentialsWithoutDefaults):
                raise LoadJobTerminalException(
                    file_path,
                    "Unsigned Azure Blob Storage access from ClickHouse isn't supported as yet.",
                )

            # Authenticated access.
            account_name = staging_credentials.azure_storage_account_name
            storage_account_url = (
                f"https://{staging_credentials.azure_storage_account_name}.blob.core.windows.net"
            )
            account_key = staging_credentials.azure_storage_account_key

            # build table func
            table_function = f"azureBlobStorage('{storage_account_url}','{bucket_url.netloc}','{bucket_url.path}','{account_name}','{account_key}','{clickhouse_format}','{compression}')"
        else:
            raise LoadJobTerminalException(
                file_path,
                f"ClickHouse loader does not support '{bucket_scheme}' filesystem.",
            )

        statement = f"INSERT INTO {qualified_table_name} SELECT * FROM {table_function}"
        with client.begin_transaction():
            client.execute_sql(statement)

    def state(self) -> TLoadJobState:
        return "completed"

    def exception(self) -> str:
        raise NotImplementedError()


class ClickHouseMergeJob(SqlMergeJob):
    @classmethod
    def _to_temp_table(cls, select_sql: str, temp_table_name: str) -> str:
        return f"CREATE TABLE {temp_table_name} ENGINE = Memory AS {select_sql};"

    @classmethod
    def gen_key_table_clauses(
        cls,
        root_table_name: str,
        staging_root_table_name: str,
        key_clauses: Sequence[str],
        for_delete: bool,
    ) -> List[str]:
        join_conditions = " AND ".join([c.format(d="d", s="s") for c in key_clauses])
        return [
            f"FROM {root_table_name} AS d JOIN {staging_root_table_name} AS s ON {join_conditions}"
        ]

    @classmethod
    def gen_update_table_prefix(cls, table_name: str) -> str:
        return f"ALTER TABLE {table_name} UPDATE"

    @classmethod
    def requires_temp_table_for_delete(cls) -> bool:
        return True


class ClickHouseClient(SqlJobClientWithStaging, SupportsStagingDestination):
    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(
        self,
        schema: Schema,
        config: ClickHouseClientConfiguration,
    ) -> None:
        self.sql_client: ClickHouseSqlClient = ClickHouseSqlClient(
            config.normalize_dataset_name(schema), config.credentials
        )
        super().__init__(schema, config, self.sql_client)
        self.config: ClickHouseClientConfiguration = config
        self.active_hints = deepcopy(HINT_TO_CLICKHOUSE_ATTR)
        self.type_mapper = ClickHouseTypeMapper(self.capabilities)

    def _create_merge_followup_jobs(self, table_chain: Sequence[TTableSchema]) -> List[NewLoadJob]:
        return [ClickHouseMergeJob.from_table_chain(table_chain, self.sql_client)]

    def _get_column_def_sql(self, c: TColumnSchema, table_format: TTableFormat = None) -> str:
        # Build column definition.
        # The primary key and sort order definition is defined outside column specification.
        hints_str = " ".join(
            self.active_hints.get(hint)
            for hint in self.active_hints.keys()
            if c.get(hint, False) is True
            and hint not in ("primary_key", "sort")
            and hint in self.active_hints
        )

        # Alter table statements only accept `Nullable` modifiers.
        # JSON type isn't nullable in ClickHouse.
        type_with_nullability_modifier = (
            f"Nullable({self.type_mapper.to_db_type(c)})"
            if c.get("nullable", True)
            else self.type_mapper.to_db_type(c)
        )

        return (
            f"{self.capabilities.escape_identifier(c['name'])} {type_with_nullability_modifier} {hints_str}"
            .strip()
        )

    def start_file_load(self, table: TTableSchema, file_path: str, load_id: str) -> LoadJob:
        return super().start_file_load(table, file_path, load_id) or ClickHouseLoadJob(
            file_path,
            table["name"],
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

        # Default to 'ReplicatedMergeTree' if user didn't explicitly set a table engine hint.
        table_type = cast(
            TTableEngineType, table.get(TABLE_ENGINE_TYPE_HINT, "replicated_merge_tree")
        )
        sql[0] = f"{sql[0]}\nENGINE = {TABLE_ENGINE_TYPE_TO_CLICKHOUSE_ATTR.get(table_type)}"

        if primary_key_list := [
            self.capabilities.escape_identifier(c["name"])
            for c in new_columns
            if c.get("primary_key")
        ]:
            sql[0] += "\nPRIMARY KEY (" + ", ".join(primary_key_list) + ")"
        else:
            sql[0] += "\nPRIMARY KEY tuple()"

        return sql

    def get_storage_table(self, table_name: str) -> Tuple[bool, TTableSchemaColumns]:
        fields = self._get_storage_table_query_columns()
        db_params = self.sql_client.make_qualified_table_name(table_name, escape=False).split(
            ".", 3
        )
        query = f'SELECT {",".join(fields)} FROM INFORMATION_SCHEMA.COLUMNS WHERE '
        if len(db_params) == 3:
            query += "table_catalog = %s AND "
        query += "table_schema = %s AND table_name = %s ORDER BY ordinal_position;"
        rows = self.sql_client.execute_sql(query, *db_params)

        # If no rows we assume that table does not exist.
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
                "nullable": bool(c[2]),
                **self._from_db_type(c[1], numeric_precision, numeric_scale),
            }
            schema_table[c[0]] = schema_c  # type: ignore
        return True, schema_table

    @staticmethod
    def _gen_not_null(v: bool) -> str:
        # ClickHouse fields are not nullable by default.
        # We use the `Nullable` modifier instead of NULL / NOT NULL modifiers to cater for ALTER statement.
        pass

    def _from_db_type(
        self, ch_t: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        return self.type_mapper.from_db_type(ch_t, precision, scale)

    def restore_file_load(self, file_path: str) -> LoadJob:
        return EmptyLoadJob.from_file_path(file_path, "completed")
