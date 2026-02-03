from typing import Any, List, Optional, Sequence, cast

from dlt.common.configuration.specs.base_configuration import CredentialsConfiguration
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.schema.schema import Schema
from dlt.common.schema.typing import TColumnSchema
from dlt.common.schema.utils import get_inherited_table_hint
from dlt.destinations.impl.clickhouse.clickhouse import (
    ClickHouseClient,
    ClickHouseLoadJob,
    ClickHouseMergeJob,
)
from dlt.destinations.impl.clickhouse_cluster.clickhouse_cluster_adapter import (
    CREATE_DISTRIBUTED_TABLES_HINT,
    DISTRIBUTED_TABLE_SUFFIX_HINT,
    DISTRIBUTED_TABLES_CONFIG_HINT_MAP,
)
from dlt.destinations.impl.clickhouse_cluster.configuration import (
    ClickHouseClusterClientConfiguration,
)
from dlt.destinations.impl.clickhouse_cluster.sql_client import ClickHouseClusterSqlClient
from dlt.destinations.sql_client import SqlClientBase


class ClickHouseClusterLoadJob(ClickHouseLoadJob):
    def __init__(
        self,
        file_path: str,
        config: ClickHouseClusterClientConfiguration,
        staging_credentials: Optional[CredentialsConfiguration] = None,
    ) -> None:
        super().__init__(file_path, config, staging_credentials)
        self._job_client: "ClickHouseClusterClient" = None

    @property
    def load_into_distributed_table(self) -> bool:
        return cast(bool, self._load_table.get(CREATE_DISTRIBUTED_TABLES_HINT, False))

    @property
    def load_table_name(self) -> str:
        return self._job_client.sql_client.get_insert_table_name(self._load_table)

    @property
    def load_database_name(self) -> str:
        if self.load_into_distributed_table:
            return self._job_client.sql_client.distributed_tables_database_name
        return super().load_database_name


class ClickHouseClusterMergeJob(ClickHouseMergeJob):
    @classmethod
    def _to_temp_table(
        cls,
        select_sql: str,
        temp_table_name: str,
        unique_column: str,
        sql_client: SqlClientBase[Any],
    ) -> str:
        if cls.DEDUP_NUMBERED_ALIAS in select_sql:
            # "CREATE TABLE AS SELECT" throws error when SELECT contains a window function applied
            # over a distributed table; to work around this, we create the temp table in two steps:
            # first create empty table, then insert into it
            create_sql = (
                sql_client._make_create_table(temp_table_name, or_replace=True)
                + f" ({unique_column} String) ENGINE = MergeTree PRIMARY KEY {unique_column}"
            )
            insert_sql = sql_client._make_insert_into(temp_table_name) + select_sql
            return f"{create_sql}; {insert_sql}"
        return super()._to_temp_table(select_sql, temp_table_name, unique_column, sql_client)

    @classmethod
    def _to_temp_table_select_name(
        cls, temp_table_name: str, sql_client: SqlClientBase[Any]
    ) -> str:
        # because temp tables are local to the node (ENGINE = MergeTree), we need to use
        # `cluster(...)` to query across cluster
        assert isinstance(sql_client, ClickHouseClusterSqlClient)
        return f"cluster({sql_client.config.cluster}, {temp_table_name})"


class ClickHouseClusterClient(ClickHouseClient):
    def __init__(
        self,
        schema: Schema,
        config: ClickHouseClusterClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(schema, config, capabilities)
        self.config: ClickHouseClusterClientConfiguration = config
        self.sql_client: ClickHouseClusterSqlClient = self.sql_client

    @property
    def sql_client_class(self) -> type[ClickHouseClusterSqlClient]:
        return ClickHouseClusterSqlClient

    @property
    def load_job_class(self) -> type[ClickHouseClusterLoadJob]:
        return ClickHouseClusterLoadJob

    @property
    def merge_job_class(self) -> type[ClickHouseClusterMergeJob]:
        return ClickHouseClusterMergeJob

    def prepare_load_table(self, table_name: str) -> PreparedTableSchema:
        table = super().prepare_load_table(table_name)

        # inherit distributed table hints if not set
        # NOTE: we don't inherit SHARDING_KEY_HINT, because it may contain columns not
        # present in child table; instead, we fall back to robust default from config
        if CREATE_DISTRIBUTED_TABLES_HINT not in table:
            table[CREATE_DISTRIBUTED_TABLES_HINT] = get_inherited_table_hint(  # type: ignore[typeddict-unknown-key]
                self.schema.tables,
                table_name,
                CREATE_DISTRIBUTED_TABLES_HINT,
                allow_none=True,
            )
        if DISTRIBUTED_TABLE_SUFFIX_HINT not in table:
            table[DISTRIBUTED_TABLE_SUFFIX_HINT] = get_inherited_table_hint(  # type: ignore[typeddict-unknown-key]
                self.schema.tables,
                table_name,
                DISTRIBUTED_TABLE_SUFFIX_HINT,
                allow_none=True,
            )

        # for hints related to distributed tables, fall back to default values from config if they
        # are still not set; we exclude dlt tables, because they are small and we don't want to
        # create distributed tables for them
        for config_key, hint_key in DISTRIBUTED_TABLES_CONFIG_HINT_MAP.items():
            if not self.schema.is_dlt_table(table["name"]) and table.get(hint_key) is None:
                table[hint_key] = self.config.get(config_key)  # type: ignore[literal-required]

        return table

    def _get_table_update_sql(
        self, table_name: str, new_columns: Sequence[TColumnSchema], generate_alter: bool
    ) -> List[str]:
        sql = super()._get_table_update_sql(table_name, new_columns, generate_alter)

        table = self.prepare_load_table(table_name)

        if table.get(CREATE_DISTRIBUTED_TABLES_HINT):
            sql.append(self.sql_client._make_create_or_replace_distributed_table(table))

        return sql
