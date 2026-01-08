import os
from typing import Optional

from dlt.destinations.impl.clickhouse.sql_client import ClickHouseSqlClient
from dlt.destinations.impl.clickhouse_cluster.configuration import (
    ClickHouseClusterClientConfiguration,
)


REPLICATED_CLUSTER_NAME = "cluster_1S_2R"
SHARDED_CLUSTER_NAME = "cluster_2S_1R"
REPLICATED_SHARDED_CLUSTER_NAME = "cluster_2S_2R"

CLICKHOUSE_CLUSTER_NODE_PORTS = (9001, 9002, 9003, 9004)
CLICKHOUSE_CLUSTER_NODE_HTTP_PORTS = (8124, 8125, 8126, 8127)


def set_clickhouse_cluster_conf(
    cluster: Optional[str] = None,
    port: Optional[int] = None,
    http_port: Optional[int] = None,
) -> None:
    if cluster is not None:
        os.environ["DESTINATION__CLICKHOUSE_CLUSTER__CLUSTER"] = cluster
    if port is not None:
        os.environ["DESTINATION__CLICKHOUSE_CLUSTER__CREDENTIALS__PORT"] = str(port)
    if http_port is not None:
        os.environ["DESTINATION__CLICKHOUSE_CLUSTER__CREDENTIALS__HTTP_PORT"] = str(http_port)


def assert_clickhouse_cluster_conf(
    config: ClickHouseClusterClientConfiguration,
    cluster: Optional[str] = None,
    port: Optional[int] = None,
    http_port: Optional[int] = None,
) -> None:
    if cluster is not None:
        assert config.cluster == cluster
    if port is not None:
        assert config.credentials.port == port
    if http_port is not None:
        assert config.credentials.http_port == http_port


def get_table_engine(sql_client: ClickHouseSqlClient, table_name: str) -> str:
    qry = "SELECT engine FROM system.tables WHERE database = %s AND name = %s;"
    table_name = sql_client.make_qualified_table_name(table_name, quote=False)
    database, name = table_name.split(".")
    with sql_client:
        result = sql_client.execute_sql(qry, database, name)

    return result[0][0]
