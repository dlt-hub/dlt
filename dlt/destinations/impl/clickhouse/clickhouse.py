from copy import deepcopy
from typing import ClassVar, Optional, Dict, List, Sequence

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import SupportsStagingDestination, TLoadJobState
from dlt.common.schema import Schema, TColumnSchema
from dlt.common.schema.typing import TTableFormat, TTableSchema, TColumnHint, TColumnType
from dlt.destinations.impl.clickhouse import capabilities
from dlt.destinations.impl.clickhouse.clickhouse_adapter import (
    TTableEngineType,
    TABLE_ENGINE_TYPE_HINT,
)
from dlt.destinations.impl.clickhouse.configuration import (
    ClickhouseClientConfiguration,
    ClickhouseCredentials,
)
from dlt.destinations.impl.clickhouse.sql_client import ClickhouseSqlClient
from dlt.destinations.job_client_impl import (
    SqlJobClientWithStaging,
    CopyRemoteFileLoadJob,
    SqlJobClientBase,
)
from dlt.destinations.sql_jobs import SqlMergeJob
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


class ClickhouseCopyFileLoadJob(CopyRemoteFileLoadJob):
    def __init__(
        self,
        table: TTableSchema,
        file_path: str,
        sql_client: ClickhouseSqlClient,
        staging_credentials: Optional[ClickhouseCredentials] = None,
        staging_iam_role: str = None,
    ) -> None:
        self._staging_iam_role = staging_iam_role
        super().__init__(table, file_path, sql_client, staging_credentials)

    def exception(self) -> str:
        pass


class ClickhouseMergeJob(SqlMergeJob):
    def __init__(self, file_name: str, status: TLoadJobState):
        super().__init__(file_name, status)


class ClickhouseClient(SqlJobClientWithStaging, SupportsStagingDestination):
    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(
        self,
        schema: Schema,
        config: ClickhouseClientConfiguration,
    ) -> None:
        self.config: ClickhouseClientConfiguration = config
        # TODO: There are no schemas in Clickhouse. No point in having schemas, only dataset names and table names for example "dataset1_mytable".
        self.sql_client = ClickhouseSqlClient(
            self.config.normalize_dataset_name(self.schema), self.config.credentials
        )
        super().__init__(schema, self.config, self.sql_client)
        self.active_hints = deepcopy(HINT_TO_CLICKHOUSE_ATTR) if self.config.create_indexes else {}
        self.type_mapper = ClickhouseTypeMapper(self.capabilities)

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
