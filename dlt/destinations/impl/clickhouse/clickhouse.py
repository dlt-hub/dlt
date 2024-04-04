import os
import re
from copy import deepcopy
from typing import ClassVar, Optional, Dict, List, Sequence, cast, Tuple
from urllib.parse import urlparse

import dlt
from dlt import config
from dlt.common.configuration.specs import (
    CredentialsConfiguration,
    AzureCredentialsWithoutDefaults,
    GcpCredentials,
    AwsCredentialsWithoutDefaults,
)
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
from dlt.common.schema.utils import (
    get_columns_names_with_prop,
    get_first_column_name_with_prop,
    get_dedup_sort_tuple,
)
from dlt.common.storages import FileStorage
from dlt.destinations.exceptions import MergeDispositionException, LoadJobTerminalException
from dlt.destinations.impl.clickhouse import capabilities
from dlt.destinations.impl.clickhouse.clickhouse_adapter import (
    TTableEngineType,
    TABLE_ENGINE_TYPE_HINT,
)
from dlt.destinations.impl.clickhouse.configuration import (
    ClickhouseClientConfiguration,
)
from dlt.destinations.impl.clickhouse.sql_client import ClickhouseSqlClient
from dlt.destinations.impl.clickhouse.utils import (
    convert_storage_to_http_scheme,
    render_object_storage_table_function,
    FILE_FORMAT_TO_TABLE_FUNCTION_MAPPING,
    SUPPORTED_FILE_FORMATS,
)
from dlt.destinations.insert_job_client import InsertValuesJobClient
from dlt.destinations.job_client_impl import (
    SqlJobClientBase,
)
from dlt.destinations.job_impl import NewReferenceJob, EmptyLoadJob
from dlt.destinations.sql_jobs import SqlMergeJob
from dlt.destinations.type_mapping import TypeMapper


HINT_TO_CLICKHOUSE_ATTR: Dict[TColumnHint, str] = {
    "primary_key": "PRIMARY KEY",
    "unique": "",  # No unique constraints available in Clickhouse.
    "foreign_key": "",  # No foreign key constraints support in Clickhouse.
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
        "timestamp": "DateTime('UTC')",
        "bigint": "Int64",
        "binary": "String",
        "wei": "Decimal",
    }

    sct_to_dbt = {
        "decimal": "Decimal(%i,%i)",
        "wei": "Decimal(%i,%i)",
        "timestamp": "DateTime(%i,'UTC')",
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

    def to_db_time_type(self, precision: Optional[int], table_format: TTableFormat = None) -> str:
        return "DateTime"

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


class ClickhouseLoadJob(LoadJob, FollowupJob):
    def __init__(
        self,
        file_path: str,
        table_name: str,
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
        file_extension = os.path.splitext(file_name)[1][
            1:
        ].lower()  # Remove dot (.) from file extension.

        if file_extension not in ["parquet", "jsonl"]:
            raise LoadJobTerminalException(
                file_path, "Clickhouse loader Only supports parquet and jsonl files."
            )

        if not config.get("data_writer.disable_compression"):
            raise LoadJobTerminalException(
                file_path,
                "Clickhouse loader does not support gzip compressed files. Please disable"
                " compression in the data writer configuration:"
                " https://dlthub.com/docs/reference/performance#disabling-and-enabling-file-compression.",
            )

        if not bucket_path:
            # Local filesystem.
            raise NotImplementedError("Only object storage is supported.")

        bucket_url = urlparse(bucket_path)
        bucket_scheme = bucket_url.scheme

        file_extension = cast(SUPPORTED_FILE_FORMATS, file_extension)
        table_function: str

        table_function = ""
        if bucket_scheme in ("s3", "gs", "gcs"):
            bucket_http_url = convert_storage_to_http_scheme(bucket_url)

            if isinstance(staging_credentials, AwsCredentialsWithoutDefaults):
                access_key_id = staging_credentials.aws_access_key_id
                secret_access_key = staging_credentials.aws_secret_access_key
            elif isinstance(staging_credentials, GcpCredentials):
                # TODO: HMAC keys aren't implemented in `GcpCredentials`.
                access_key_id = dlt.config["destination.filesystem.credentials.gcp_access_key_id"]
                secret_access_key = dlt.config[
                    "destination.filesystem.credentials.gcp_secret_access_key"
                ]
            else:
                access_key_id = None
                secret_access_key = None

            table_function = render_object_storage_table_function(
                bucket_http_url, access_key_id, secret_access_key, file_format=file_extension
            )

        elif bucket_scheme in ("az", "abfs"):
            if not isinstance(staging_credentials, AzureCredentialsWithoutDefaults):
                raise LoadJobTerminalException(
                    file_path,
                    "Unsigned Azure Blob Storage access from Clickhouse isn't supported as yet.",
                )

            # Authenticated access.
            account_name = staging_credentials.azure_storage_account_name
            storage_account_url = (
                f"https://{staging_credentials.azure_storage_account_name}.blob.core.windows.net"
            )
            account_key = staging_credentials.azure_storage_account_key
            container_name = bucket_url.netloc
            blobpath = bucket_url.path

            clickhouse_format = FILE_FORMAT_TO_TABLE_FUNCTION_MAPPING[file_extension]

            table_function = (
                f"azureBlobStorage('{storage_account_url}','{container_name}','{ blobpath }','{ account_name }','{ account_key }','{ clickhouse_format}')"
            )

        with client.begin_transaction():
            client.execute_sql(
                f"""INSERT INTO {qualified_table_name} SELECT * FROM {table_function}"""
            )

    def state(self) -> TLoadJobState:
        return "completed"

    def exception(self) -> str:
        raise NotImplementedError()


class ClickhouseMergeJob(SqlMergeJob):
    @classmethod
    def _to_temp_table(cls, select_sql: str, temp_table_name: str) -> str:
        # Different sessions are created during the load process, and temporary tables
        # do not persist between sessions.
        # Resorting to persisted in-memory table to fix.
        # return f"CREATE TABLE {temp_table_name} ENGINE = Memory AS {select_sql};"
        return f"CREATE TABLE {temp_table_name} ENGINE = Memory AS {select_sql};"

    @classmethod
    def gen_merge_sql(
        cls, table_chain: Sequence[TTableSchema], sql_client: ClickhouseSqlClient  # type: ignore[override]
    ) -> List[str]:
        sql: List[str] = []
        root_table = table_chain[0]

        escape_id = sql_client.capabilities.escape_identifier
        escape_lit = sql_client.capabilities.escape_literal
        if escape_id is None:
            escape_id = DestinationCapabilitiesContext.generic_capabilities().escape_identifier
        if escape_lit is None:
            escape_lit = DestinationCapabilitiesContext.generic_capabilities().escape_literal

        root_table_name = sql_client.make_qualified_table_name(root_table["name"])
        with sql_client.with_staging_dataset(staging=True):
            staging_root_table_name = sql_client.make_qualified_table_name(root_table["name"])
        primary_keys = list(
            map(
                escape_id,
                get_columns_names_with_prop(root_table, "primary_key"),
            )
        )
        merge_keys = list(
            map(
                escape_id,
                get_columns_names_with_prop(root_table, "merge_key"),
            )
        )
        key_clauses = cls._gen_key_table_clauses(primary_keys, merge_keys)

        unique_column: str = None
        root_key_column: str = None

        if len(table_chain) == 1:
            key_table_clauses = cls.gen_key_table_clauses(
                root_table_name, staging_root_table_name, key_clauses, for_delete=True
            )
            sql.extend(f"DELETE {clause};" for clause in key_table_clauses)
        else:
            key_table_clauses = cls.gen_key_table_clauses(
                root_table_name, staging_root_table_name, key_clauses, for_delete=False
            )
            unique_columns = get_columns_names_with_prop(root_table, "unique")
            if not unique_columns:
                raise MergeDispositionException(
                    sql_client.fully_qualified_dataset_name(),
                    staging_root_table_name,
                    [t["name"] for t in table_chain],
                    f"There is no unique column (ie _dlt_id) in top table {root_table['name']} so"
                    " it is not possible to link child tables to it.",
                )
            unique_column = escape_id(unique_columns[0])
            create_delete_temp_table_sql, delete_temp_table_name = cls.gen_delete_temp_table_sql(
                unique_column, key_table_clauses
            )
            sql.extend(create_delete_temp_table_sql)

            for table in table_chain[1:]:
                table_name = sql_client.make_qualified_table_name(table["name"])
                root_key_columns = get_columns_names_with_prop(table, "root_key")
                if not root_key_columns:
                    raise MergeDispositionException(
                        sql_client.fully_qualified_dataset_name(),
                        staging_root_table_name,
                        [t["name"] for t in table_chain],
                        "There is no root foreign key (ie _dlt_root_id) in child table"
                        f" {table['name']} so it is not possible to refer to top level table"
                        f" {root_table['name']} unique column {unique_column}",
                    )
                root_key_column = escape_id(root_key_columns[0])
                sql.append(
                    cls.gen_delete_from_sql(
                        table_name, root_key_column, delete_temp_table_name, unique_column
                    )
                )

            sql.append(
                cls.gen_delete_from_sql(
                    root_table_name, unique_column, delete_temp_table_name, unique_column
                )
            )

        not_deleted_cond: str = None
        hard_delete_col = get_first_column_name_with_prop(root_table, "hard_delete")
        if hard_delete_col is not None:
            not_deleted_cond = f"{escape_id(hard_delete_col)} IS NULL"
            if root_table["columns"][hard_delete_col]["data_type"] == "bool":
                not_deleted_cond += f" OR {escape_id(hard_delete_col)} = {escape_lit(False)}"

        dedup_sort = get_dedup_sort_tuple(root_table)

        insert_temp_table_name: str = None
        if len(table_chain) > 1 and (primary_keys or hard_delete_col is not None):
            condition_columns = [hard_delete_col] if not_deleted_cond is not None else None
            (
                create_insert_temp_table_sql,
                insert_temp_table_name,
            ) = cls.gen_insert_temp_table_sql(
                staging_root_table_name,
                primary_keys,
                unique_column,
                dedup_sort,
                not_deleted_cond,
                condition_columns,
            )
            sql.extend(create_insert_temp_table_sql)

        to_delete: List[str] = []

        for table in table_chain:
            table_name = sql_client.make_qualified_table_name(table["name"])
            with sql_client.with_staging_dataset(staging=True):
                staging_table_name = sql_client.make_qualified_table_name(table["name"])

            insert_cond = not_deleted_cond if hard_delete_col is not None else "1 = 1"
            if (
                primary_keys
                and len(table_chain) > 1
                or not primary_keys
                and table.get("parent") is not None
                and hard_delete_col is not None
            ):
                uniq_column = unique_column if table.get("parent") is None else root_key_column
                insert_cond = f"{uniq_column} IN (SELECT * FROM {insert_temp_table_name})"

            columns = list(map(escape_id, get_columns_names_with_prop(table, "name")))
            col_str = ", ".join(columns)
            select_sql = f"SELECT {col_str} FROM {staging_table_name} WHERE {insert_cond}"
            if primary_keys and len(table_chain) == 1:
                select_sql = cls.gen_select_from_dedup_sql(
                    staging_table_name, primary_keys, columns, dedup_sort, insert_cond
                )

            sql.extend([f"INSERT INTO {table_name}({col_str}) {select_sql};"])

            if table_name is not None and table_name.startswith("delete_"):
                to_delete.extend([table_name])
            if insert_temp_table_name is not None and insert_temp_table_name.startswith("delete_"):
                to_delete.extend([insert_temp_table_name])

        # TODO: Doesn't remove all `delete_` tables.
        for delete_table_name in to_delete:
            sql.extend(
                [f"DROP TABLE IF EXISTS {sql_client.make_qualified_table_name(delete_table_name)};"]
            )

        return sql


class ClickhouseClient(InsertValuesJobClient, SupportsStagingDestination):
    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(
        self,
        schema: Schema,
        config: ClickhouseClientConfiguration,
    ) -> None:
        self.sql_client: ClickhouseSqlClient = ClickhouseSqlClient(
            config.normalize_dataset_name(schema), config.credentials
        )
        super().__init__(schema, config, self.sql_client)
        self.config: ClickhouseClientConfiguration = config
        self.active_hints = deepcopy(HINT_TO_CLICKHOUSE_ATTR)
        self.type_mapper = ClickhouseTypeMapper(self.capabilities)

    def _create_merge_followup_jobs(self, table_chain: Sequence[TTableSchema]) -> List[NewLoadJob]:
        return [ClickhouseMergeJob.from_table_chain(table_chain, self.sql_client)]

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
        # JSON type isn't nullable in Clickhouse.
        type_with_nullability_modifier = (
            f"Nullable({self.type_mapper.to_db_type(c)})"
            if c.get("nullable", True) and c.get("data_type") != "complex"
            else self.type_mapper.to_db_type(c)
        )

        return (
            f"{self.capabilities.escape_identifier(c['name'])} {type_with_nullability_modifier} {hints_str}"
            .strip()
        )

    def start_file_load(self, table: TTableSchema, file_path: str, load_id: str) -> LoadJob:
        job = super().start_file_load(table, file_path, load_id) or ClickhouseLoadJob(
            file_path,
            table["name"],
            self.sql_client,
            staging_credentials=(
                self.config.staging_config.credentials if self.config.staging_config else None
            ),
        )
        if not job:
            assert NewReferenceJob.is_reference_job(
                file_path
            ), "Clickhouse must use staging to load files."
        return job

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

        # TODO: Apply sort order and cluster key hints.

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

    # Clickhouse fields are not nullable by default.

    @staticmethod
    def _gen_not_null(v: bool) -> str:
        # We use the `Nullable` modifier instead of NULL / NOT NULL modifiers to cater for ALTER statement.
        pass

    def _from_db_type(
        self, ch_t: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        return self.type_mapper.from_db_type(ch_t, precision, scale)

    def restore_file_load(self, file_path: str) -> LoadJob:
        return EmptyLoadJob.from_file_path(file_path, "completed")
