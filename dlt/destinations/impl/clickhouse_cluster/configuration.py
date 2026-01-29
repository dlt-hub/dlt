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
    """Comma-separated list of alternative host:port pairs.

    Used as fallback when connecting to `host`:`port` fails. Example: `host1:9441,host2:9440`.
    """
    alt_http_hosts: Optional[str] = None
    """Comma-separated list of alternative host:http_port pairs.

    Used as fallback when connecting to `host`:`http_port` fails. Example: `host1:8444,host2:8443`.
    """

    __query_params__: ClassVar[List[str]] = ClickHouseCredentials.__query_params__ + ["alt_hosts"]

    __session_settings__: ClassVar[Dict[str, Any]] = ClickHouseCredentials.__session_settings__ | {
        # NOTE: ClickHouse' default setting do not guarantee strong consistency when working with
        # replicated tables; these settings increase consistency. We err on the side of correctness,
        # not performance.
        # https://blog.sentry.io/how-to-get-stronger-consistency-out-of-a-datastore/#how-do-we-ensure-we-read-from-an-up-to-date-replica-3
        "mutations_sync": 2,
        "insert_distributed_sync": 1,
        "lightweight_deletes_sync": 2,
        "select_sequential_consistency": 1,
        "load_balancing": "in_order",
    }

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
        if "alt_hosts" in self.query:
            self.alt_hosts = self.query["alt_hosts"]


@configspec
class ClickHouseClusterClientConfiguration(ClickHouseClientConfiguration):
    # override `clickhouse` attributes
    credentials: ClickHouseClusterCredentials = None
    # NOTE: `replicated_merge_tree` makes sense for dlt tables because they are small
    dlt_tables_table_engine_type: TTableEngineType = "replicated_merge_tree"
    """Default table engine to use for dlt tables.

    Also applies to dataset sentinel table. Falls back to `table_engine_type` if set to `None`.
    """

    # add `clickhouse_cluster` specific attributes
    cluster: str = None
    """Name of the ClickHouse cluster to load data into."""
    create_distributed_tables: bool = False
    """Whether to create distributed tables in addition to standard tables.

    Can be overridden per resource using `clickhouse_cluster_adapter`.
    """
    distributed_tables_database: Optional[str] = None
    """Name of the database to create distributed tables in.

    If set to `None`, uses the same database as standard tables.
    """
    distributed_table_suffix: str = DEFAULT_DISTRIBUTED_TABLE_SUFFIX
    """Suffix to append to table names when creating distributed tables.

    For example, if set to `_dist`, a table named `events` will have a distributed table named
    `events_dist`.

    Can be overridden per resource using `clickhouse_cluster_adapter`.
    """
    sharding_key: str = DEFAULT_SHARDING_KEY
    """Sharding key expression to use for distributed tables.

    Can be overridden per resource using `clickhouse_cluster_adapter`.
    """
