import os
import subprocess
from contextlib import contextmanager
from typing import Literal, Optional, Sequence

import pytest

from dlt.common.schema.schema import Schema
from dlt.common.utils import uniq_id
from dlt.destinations.impl.clickhouse.sql_client import ClickHouseSqlClient
from dlt.destinations.impl.clickhouse_cluster.clickhouse_cluster import ClickHouseClusterClient
from dlt.destinations.impl.clickhouse_cluster.configuration import (
    ClickHouseClusterClientConfiguration,
)
from dlt.destinations.impl.clickhouse_cluster.factory import clickhouse_cluster


REPLICATED_CLUSTER_NAME = "cluster_1S_2R"
SHARDED_CLUSTER_NAME = "cluster_2S_1R"
REPLICATED_SHARDED_CLUSTER_NAME = "cluster_2S_2R"

CLICKHOUSE_CLUSTER_NODES = (
    "clickhouse-01",
    "clickhouse-02",
    "clickhouse-03",
    "clickhouse-04",
)
CLICKHOUSE_CLUSTER_NODE_PORTS = (9001, 9002, 9003, 9004)
CLICKHOUSE_CLUSTER_NODE_HTTP_PORTS = (8124, 8125, 8126, 8127)

TClickHouseDriver = Literal["clickhouse_driver", "clickhouse_connect"]


@pytest.fixture
def client(empty_schema: Schema) -> ClickHouseClusterClient:
    # return client without opening connection
    return clickhouse_cluster().client(
        empty_schema,
        ClickHouseClusterClientConfiguration()._bind_dataset_name(dataset_name="test_" + uniq_id()),
    )


def set_clickhouse_cluster_conf(
    cluster: Optional[str] = None,
    port: Optional[int] = None,
    http_port: Optional[int] = None,
    alt_ports: Optional[Sequence[int]] = None,
    alt_http_ports: Optional[Sequence[int]] = None,
) -> None:
    env_var_prefix = "DESTINATION__CLICKHOUSE_CLUSTER__"
    if cluster is not None:
        os.environ[env_var_prefix + "CLUSTER"] = cluster
    if port is not None:
        os.environ[env_var_prefix + "CREDENTIALS__PORT"] = str(port)
    if http_port is not None:
        os.environ[env_var_prefix + "CREDENTIALS__HTTP_PORT"] = str(http_port)
    if alt_ports is not None:
        os.environ[env_var_prefix + "CREDENTIALS__ALT_PORTS"] = str(alt_ports)
    if alt_http_ports is not None:
        os.environ[env_var_prefix + "CREDENTIALS__ALT_HTTP_PORTS"] = str(alt_http_ports)


def assert_clickhouse_cluster_conf(
    config: ClickHouseClusterClientConfiguration,
    cluster: Optional[str] = None,
    port: Optional[int] = None,
    http_port: Optional[int] = None,
    alt_ports: Optional[Sequence[int]] = None,
    alt_http_ports: Optional[Sequence[int]] = None,
) -> None:
    if cluster is not None:
        assert config.cluster == cluster
    if port is not None:
        assert config.credentials.port == port
    if http_port is not None:
        assert config.credentials.http_port == http_port
    if alt_ports is not None:
        assert config.credentials.alt_ports == alt_ports
    if alt_http_ports is not None:
        assert config.credentials.alt_http_ports == alt_http_ports


def get_table_engine(sql_client: ClickHouseSqlClient, table_name: str, full: bool = False) -> str:
    col = "engine_full" if full else "engine"
    qry = f"SELECT {col} FROM system.tables WHERE database = %s AND name = %s;"
    table_name = sql_client.make_qualified_table_name(table_name, quote=False)
    database, name = table_name.split(".")
    with sql_client:
        result = sql_client.execute_sql(qry, database, name)

    return result[0][0]


def query(
    sql_client: ClickHouseSqlClient,
    qry: str,
    driver=TClickHouseDriver,
):
    if driver == "clickhouse_driver":
        with sql_client:
            return sql_client.execute_sql(qry)
    elif driver == "clickhouse_connect":
        with sql_client.clickhouse_connect_client() as clickhouse_connect_client:
            return clickhouse_connect_client.query(qry).result_rows


def get_node_name(sql_client: ClickHouseSqlClient, driver=TClickHouseDriver) -> str:
    """Returns the name (e.g. '01' or '02') of the ClickHouse cluster node the driver is connected to.

    Relies on `node` macro defined in `config.xml` of each ClickHouse node.
    """

    return query(sql_client, qry="SELECT getMacro('node');", driver=driver)[0][0]


def get_clickhouse_cluster_node_container_name(node: int) -> str:
    return CLICKHOUSE_CLUSTER_NODES[node - 1]


@contextmanager
def clickhouse_cluster_node_paused(*node: int):
    container = get_clickhouse_cluster_node_container_name

    # pause docker containers
    for n in node:
        subprocess.run(["docker", "pause", container(n)], capture_output=True, check=True)
    try:
        yield
    finally:
        # unpause docker containers
        for n in node:
            subprocess.run(["docker", "unpause", container(n)], capture_output=True, check=True)
