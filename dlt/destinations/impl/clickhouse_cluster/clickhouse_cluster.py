from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.schema.schema import Schema
from dlt.destinations.impl.clickhouse.clickhouse import ClickHouseClient
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
