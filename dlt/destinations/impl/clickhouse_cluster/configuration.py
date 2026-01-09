from dlt.common.configuration import configspec
from dlt.destinations.impl.clickhouse.configuration import ClickHouseClientConfiguration


DEFAULT_DISTRIBUTED_TABLE_SUFFIX = "_dist"
DEFAULT_SHARDING_KEY = "rand()"


@configspec
class ClickHouseClusterClientConfiguration(ClickHouseClientConfiguration):
    cluster: str = None
    create_distributed_tables: bool = False
    distributed_table_suffix: str = DEFAULT_DISTRIBUTED_TABLE_SUFFIX
    sharding_key: str = DEFAULT_SHARDING_KEY
