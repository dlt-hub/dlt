from typing import List, Optional

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.typing import PreparedTableSchema
from dlt.destinations.impl.clickhouse.configuration import ClickHouseCredentials
from dlt.destinations.impl.clickhouse.sql_client import ClickHouseSqlClient
from dlt.destinations.impl.clickhouse_cluster.clickhouse_cluster_adapter import (
    DISTRIBUTED_TABLE_SUFFIX_HINT,
)
from dlt.destinations.impl.clickhouse_cluster.configuration import (
    ClickHouseClusterClientConfiguration,
)


class ClickHouseClusterSqlClient(ClickHouseSqlClient):
    def __init__(
        self,
        dataset_name: Optional[str],
        staging_dataset_name: str,
        known_table_names: List[str],
        credentials: ClickHouseCredentials,
        capabilities: DestinationCapabilitiesContext,
        config: ClickHouseClusterClientConfiguration,
    ) -> None:
        super().__init__(
            dataset_name,
            staging_dataset_name,
            known_table_names,
            credentials,
            capabilities,
            config,
        )
        self.config: ClickHouseClusterClientConfiguration = config

    def _make_create_table(
        self, qualified_name: str, or_replace: bool = False, if_not_exists: bool = False
    ) -> str:
        create_table_sql = super()._make_create_table(qualified_name, or_replace, if_not_exists)
        return f"{create_table_sql} ON CLUSTER {self.config.cluster}"

    def _make_drop_table(self, qualified_table_name: str, if_exists: bool = False) -> str:
        if_exists_sql = "IF EXISTS " if if_exists else ""
        cluster = self.config.cluster
        return f"DROP TABLE {if_exists_sql}{qualified_table_name} ON CLUSTER {cluster} SYNC"

    def _make_create_distributed_table(self, table_schema: PreparedTableSchema) -> str:
        table_name = table_schema["name"]

        # generate CREATE TABLE sql
        dist_table_name = table_name + table_schema[DISTRIBUTED_TABLE_SUFFIX_HINT]  # type: ignore[typeddict-item]
        qual_dist_table_name = self.make_qualified_table_name(dist_table_name)
        create_table_sql = self._make_create_table(qual_dist_table_name)

        # generate ENGINE clause
        cluster = self.config.cluster
        database, table = self.make_qualified_table_name(table_name).split(".")
        engine_sql = self._make_distributed_engine_clause(cluster, database, table)

        return f"{create_table_sql} {engine_sql};"

    @staticmethod
    def _make_distributed_engine_clause(cluster: str, database: str, table: str) -> str:
        return f"ENGINE = Distributed('{cluster}', {database}, {table}, rand())"
