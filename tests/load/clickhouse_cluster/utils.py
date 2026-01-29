import os
import subprocess
from contextlib import contextmanager
from typing import Literal, Optional

import pytest

from dlt.common.schema.schema import Schema
from dlt.common.utils import uniq_id
from dlt.destinations.impl.clickhouse.sql_client import ClickHouseSqlClient
from dlt.destinations.impl.clickhouse.typing import TTableEngineType
from dlt.destinations.impl.clickhouse_cluster.clickhouse_cluster import ClickHouseClusterClient
from dlt.destinations.impl.clickhouse_cluster.configuration import (
    ClickHouseClusterClientConfiguration,
)
from dlt.destinations.impl.clickhouse_cluster.factory import clickhouse_cluster
from dlt.destinations.impl.clickhouse_cluster.sql_client import ClickHouseClusterSqlClient


REPLICATED_CLUSTER_NAME = "cluster_1S_2R"
SHARDED_CLUSTER_NAME = "cluster_2S_1R"
REPLICATED_SHARDED_CLUSTER_NAME = "cluster_2S_2R"

CLICKHOUSE_CLUSTER_HOST = "localhost"
CLICKHOUSE_CLUSTER_DATABASE = "dlt_data"
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
    distributed_tables_database: Optional[str] = None,
    table_engine_type: Optional[TTableEngineType] = None,
    port: Optional[int] = None,
    http_port: Optional[int] = None,
    alt_hosts: Optional[str] = None,
    alt_http_hosts: Optional[str] = None,
) -> None:
    env_var_prefix = "DESTINATION__CLICKHOUSE_CLUSTER__"
    if cluster is not None:
        os.environ[env_var_prefix + "CLUSTER"] = cluster
    if distributed_tables_database is not None:
        os.environ[env_var_prefix + "DISTRIBUTED_TABLES_DATABASE"] = distributed_tables_database
    if table_engine_type is not None:
        os.environ[env_var_prefix + "TABLE_ENGINE_TYPE"] = table_engine_type
    if port is not None:
        os.environ[env_var_prefix + "CREDENTIALS__PORT"] = str(port)
    if http_port is not None:
        os.environ[env_var_prefix + "CREDENTIALS__HTTP_PORT"] = str(http_port)
    if alt_hosts is not None:
        os.environ[env_var_prefix + "CREDENTIALS__ALT_HOSTS"] = alt_hosts
    if alt_http_hosts is not None:
        os.environ[env_var_prefix + "CREDENTIALS__ALT_HTTP_HOSTS"] = alt_http_hosts


def assert_clickhouse_cluster_conf(
    config: ClickHouseClusterClientConfiguration,
    cluster: Optional[str] = None,
    distributed_tables_database: Optional[str] = None,
    table_engine_type: Optional[TTableEngineType] = None,
    port: Optional[int] = None,
    http_port: Optional[int] = None,
    alt_hosts: Optional[str] = None,
    alt_http_hosts: Optional[str] = None,
) -> None:
    if cluster is not None:
        assert config.cluster == cluster
    if distributed_tables_database is not None:
        assert config.distributed_tables_database == distributed_tables_database
    if table_engine_type is not None:
        assert config.table_engine_type == table_engine_type
    if port is not None:
        assert config.credentials.port == port
    if http_port is not None:
        assert config.credentials.http_port == http_port
    if alt_hosts is not None:
        assert config.credentials.alt_hosts == alt_hosts
    if alt_http_hosts is not None:
        assert config.credentials.alt_http_hosts == alt_http_hosts


def query_system_table(
    sql_client: ClickHouseSqlClient,
    table_name: str,
    alternative_database_name: Optional[str] = None,
    projection: str = "*",
):
    qry = f"SELECT {projection} FROM system.tables WHERE database = %s AND name = %s;"
    database_name = alternative_database_name or sql_client.database_name
    with sql_client.with_alternative_database_name(database_name):
        table_name = sql_client.make_qualified_table_name(table_name, quote=False)
    database, name = table_name.split(".")
    if sql_client._conn:
        rows = sql_client.execute_sql(qry, database, name)
    else:
        with sql_client:
            rows = sql_client.execute_sql(qry, database, name)
    return rows


def table_exists(
    sql_client: ClickHouseSqlClient,
    table_name: str,
    alternative_database_name: Optional[str] = None,
) -> bool:
    projection = "count()"
    rows = query_system_table(sql_client, table_name, alternative_database_name, projection)
    return rows[0][0] > 0


def get_table_engine(
    sql_client: ClickHouseSqlClient,
    table_name: str,
    full: bool = False,
    alternative_database_name: Optional[str] = None,
) -> str:
    projection = "engine_full" if full else "engine"
    rows = query_system_table(sql_client, table_name, alternative_database_name, projection)
    return rows[0][0]


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


def get_node_port(node: int, http: bool = False) -> int:
    return (
        CLICKHOUSE_CLUSTER_NODE_HTTP_PORTS[node - 1]
        if http
        else CLICKHOUSE_CLUSTER_NODE_PORTS[node - 1]
    )


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


@contextmanager
def clickhouse_cluster_database_created(
    sql_client: ClickHouseClusterSqlClient,
    database_name: str,
):
    # create database
    with sql_client:
        sql_client.execute_sql(
            f"CREATE DATABASE IF NOT EXISTS `{database_name}` ON CLUSTER"
            f" {sql_client.config.cluster};"
        )
    try:
        yield
    finally:
        # drop database (except if it's the main test database)
        if database_name != CLICKHOUSE_CLUSTER_DATABASE:
            with sql_client:
                sql_client.execute_sql(
                    f"DROP DATABASE `{database_name}` ON CLUSTER {sql_client.config.cluster} SYNC;"
                )
