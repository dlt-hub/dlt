from typing import TYPE_CHECKING

from dlt.common.destination import Destination
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.destinations.impl.clickhouse.factory import clickhouse as clickhouse_factory
from dlt.destinations.impl.clickhouse_cluster.configuration import (
    ClickHouseClusterClientConfiguration,
)

if TYPE_CHECKING:
    from dlt.destinations.impl.clickhouse_cluster.clickhouse_cluster import ClickHouseClusterClient


class clickhouse_cluster(
    Destination[ClickHouseClusterClientConfiguration, "ClickHouseClusterClient"]
):
    spec = ClickHouseClusterClientConfiguration

    @property
    def client_class(self) -> type["ClickHouseClusterClient"]:
        from dlt.destinations.impl.clickhouse_cluster.clickhouse_cluster import (
            ClickHouseClusterClient,
        )

        return ClickHouseClusterClient

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()
        caps = clickhouse_factory._set_raw_capabilities(caps)
        return caps
