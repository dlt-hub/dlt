from typing import Any, ClassVar, Dict, List, Optional, Tuple
from dlt.common.configuration import configspec
from dlt.destinations.impl.clickhouse.configuration import (
    ClickHouseClientConfiguration,
    ClickHouseCredentials,
)
from dlt.destinations.impl.clickhouse.typing import TTableEngineType


DEFAULT_DISTRIBUTED_TABLE_SUFFIX = "_dist"
DEFAULT_SHARDING_KEY = "rand()"


@configspec(init=False)
class ClickHouseClusterCredentials(ClickHouseCredentials):
    alt_hosts: Optional[str] = None
    alt_http_hosts: Optional[str] = None

    __query_params__: ClassVar[List[str]] = ["alt_hosts"]

    @property
    def _http_hosts(self) -> List[Tuple[str, int]]:
        """Returns list of configured hosts used to connect to ClickHouse cluster over HTTP.

        Each host is represented as (host, port) tuple.
        Starts with primary host, followed by alternative hosts (if any).
        """

        hosts = [(self.host, self.http_port)]
        if self.alt_http_hosts:
            hosts += [
                (host, int(port))
                for host_port in self.alt_http_hosts.split(",")
                for host, port in [host_port.split(":")]
            ]
        return hosts

    def parse_native_representation(self, native_value: Any) -> None:
        super().parse_native_representation(native_value)
        for param in self.__query_params__:
            if param in self.query:
                setattr(self, param, self.query[param])

    def get_query(self) -> Dict[str, Any]:
        query = super().get_query()
        for param in self.__query_params__:
            if self.get(param) is not None:
                query[param] = self[param]
        return query


@configspec
class ClickHouseClusterClientConfiguration(ClickHouseClientConfiguration):
    # override `clickhouse` attributes
    credentials: ClickHouseClusterCredentials = None
    # NOTE: `replicated_merge_tree` makes sense for dlt tables because they are small
    dlt_tables_table_engine_type: TTableEngineType = "replicated_merge_tree"
    """Default table engine to use for dlt tables. Also applies to dataset sentinel table. Falls back to `table_engine_type` if set to `None`."""

    # add `clickhouse_cluster` specific attributes
    cluster: str = None
    create_distributed_tables: bool = False
    distributed_tables_database: Optional[str] = None
    distributed_table_suffix: str = DEFAULT_DISTRIBUTED_TABLE_SUFFIX
    sharding_key: str = DEFAULT_SHARDING_KEY
