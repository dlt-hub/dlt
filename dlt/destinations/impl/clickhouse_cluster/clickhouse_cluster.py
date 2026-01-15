from typing import List, Sequence, cast

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.schema.schema import Schema
from dlt.common.schema.typing import TColumnSchema
from dlt.common.schema.utils import get_inherited_table_hint
from dlt.destinations.impl.clickhouse.clickhouse import ClickHouseClient
from dlt.destinations.impl.clickhouse_cluster.clickhouse_cluster_adapter import (
    CONFIG_HINT_MAP,
    CREATE_DISTRIBUTED_TABLE_HINT,
    DISTRIBUTED_TABLE_SUFFIX_HINT,
)
from dlt.destinations.impl.clickhouse_cluster.configuration import (
    ClickHouseClusterClientConfiguration,
)
from dlt.destinations.impl.clickhouse_cluster.sql_client import ClickHouseClusterSqlClient


class ClickHouseClusterClient(ClickHouseClient):
    def __init__(
        self,
        schema: Schema,
        config: ClickHouseClusterClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(schema, config, capabilities)
        self.config: ClickHouseClusterClientConfiguration = config

    @property
    def sql_client_class(self) -> type[ClickHouseClusterSqlClient]:
        return ClickHouseClusterSqlClient

    def prepare_load_table(self, table_name: str) -> PreparedTableSchema:
        table = super().prepare_load_table(table_name)

        # inherit distributed table hints if not set
        # NOTE: we don't inherit SHARDING_KEY_HINT, because it may contain columns not
        # present in child table; instead, we fall back to robust default from config
        if CREATE_DISTRIBUTED_TABLE_HINT not in table:
            table[CREATE_DISTRIBUTED_TABLE_HINT] = get_inherited_table_hint(  # type: ignore[typeddict-unknown-key]
                self.schema.tables,
                table_name,
                CREATE_DISTRIBUTED_TABLE_HINT,
                allow_none=True,
            )
        if DISTRIBUTED_TABLE_SUFFIX_HINT not in table:
            table[DISTRIBUTED_TABLE_SUFFIX_HINT] = get_inherited_table_hint(  # type: ignore[typeddict-unknown-key]
                self.schema.tables,
                table_name,
                DISTRIBUTED_TABLE_SUFFIX_HINT,
                allow_none=True,
            )

        # fall back to default values from config if hints are still not set
        for config_key, hint_key in CONFIG_HINT_MAP.items():
            if table.get(hint_key) is None:
                table[hint_key] = self.config.get(config_key)  # type: ignore[literal-required]

        return table

    def _get_table_update_sql(
        self, table_name: str, new_columns: Sequence[TColumnSchema], generate_alter: bool
    ) -> List[str]:
        sql = super()._get_table_update_sql(table_name, new_columns, generate_alter)

        table = self.prepare_load_table(table_name)

        if table.get(CREATE_DISTRIBUTED_TABLE_HINT):
            sql_client = cast(ClickHouseClusterSqlClient, self.sql_client)
            create_dist_table_sql = sql_client._make_create_distributed_table(table)
            sql.append(create_dist_table_sql)

        return sql
