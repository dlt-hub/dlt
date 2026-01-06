from dlt.common.configuration import configspec
from dlt.destinations.impl.clickhouse.configuration import ClickHouseClientConfiguration


@configspec
class ClickHouseClusterClientConfiguration(ClickHouseClientConfiguration):
    cluster: str = None
