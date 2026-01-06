from typing import List, Optional

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.destinations.impl.clickhouse.configuration import ClickHouseCredentials
from dlt.destinations.impl.clickhouse.sql_client import ClickHouseSqlClient
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
