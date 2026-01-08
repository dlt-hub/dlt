from typing import List, Sequence, cast

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.schema.schema import Schema
from dlt.common.schema.typing import TColumnSchema
from dlt.destinations.impl.clickhouse.clickhouse import ClickHouseClient, ClickHouseLoadJob
from dlt.destinations.impl.clickhouse_cluster.clickhouse_cluster_adapter import (
    CREATE_DISTRIBUTED_TABLE_HINT,
    DISTRIBUTED_TABLE_SUFFIX_HINT,
)
from dlt.destinations.impl.clickhouse_cluster.configuration import (
    ClickHouseClusterClientConfiguration,
)
from dlt.destinations.impl.clickhouse_cluster.sql_client import ClickHouseClusterSqlClient


class ClickHouseClusterLoadJob(ClickHouseLoadJob):
    @property
    def load_table_name(self) -> str:
        name = self._load_table["name"]
        if self._load_table.get(CREATE_DISTRIBUTED_TABLE_HINT):
            name += self._load_table[DISTRIBUTED_TABLE_SUFFIX_HINT]  # type: ignore[typeddict-item]
        return name


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

    @property
    def load_job_class(self) -> type[ClickHouseClusterLoadJob]:
        return ClickHouseClusterLoadJob

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
