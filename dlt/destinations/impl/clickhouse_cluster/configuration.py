from typing import Any, Dict, List, Optional, Sequence
from dlt.common.configuration import configspec
from dlt.destinations.impl.clickhouse.configuration import (
    ClickHouseClientConfiguration,
    ClickHouseCredentials,
)


DEFAULT_DISTRIBUTED_TABLE_SUFFIX = "_dist"
DEFAULT_SHARDING_KEY = "rand()"


@configspec(init=False)
class ClickHouseClusterCredentials(ClickHouseCredentials):
    alt_ports: Optional[Sequence[int]] = None
    alt_http_ports: Optional[Sequence[int]] = None

    @property
    def _http_ports(self) -> List[int]:
        """Returns list of configured HTTP ports used to connect to ClickHouse cluster.

        Starts with primary port, followed by alternative ports (if any).
        """

        alt_http_ports = self.alt_http_ports or []
        return [self.http_port] + [p for p in alt_http_ports if p != self.http_port]

    def get_query(self) -> Dict[str, Any]:
        query = super().get_query()

        if self.alt_ports:
            query["alt_hosts"] = ",".join(f"{self.host}:{port}" for port in self.alt_ports)

        return query


@configspec
class ClickHouseClusterClientConfiguration(ClickHouseClientConfiguration):
    credentials: ClickHouseClusterCredentials = None
    cluster: str = None
    create_distributed_tables: bool = False
    distributed_table_suffix: str = DEFAULT_DISTRIBUTED_TABLE_SUFFIX
    sharding_key: str = DEFAULT_SHARDING_KEY
