import os
from typing import cast
import pytest

import dlt
from dlt.destinations.adapters import clickhouse_cluster_adapter
from dlt.destinations.impl.clickhouse.sql_client import ClickHouseSqlClient
from dlt.destinations.impl.clickhouse_cluster.clickhouse_cluster_adapter import (
    DEFAULT_DISTRIBUTED_TABLE_SUFFIX,
)
from dlt.destinations.impl.clickhouse_cluster.configuration import (
    ClickHouseClusterClientConfiguration,
)
from tests.load.clickhouse_cluster.utils import (
    CLICKHOUSE_CLUSTER_NODE_HTTP_PORTS,
    CLICKHOUSE_CLUSTER_NODE_PORTS,
    REPLICATED_CLUSTER_NAME,
    REPLICATED_SHARDED_CLUSTER_NAME,
    SHARDED_CLUSTER_NAME,
    assert_clickhouse_cluster_conf,
    get_table_engine,
    set_clickhouse_cluster_conf,
)
from tests.load.utils import DestinationTestConfiguration, destinations_configs
from tests.pipeline.utils import assert_load_info


# NOTE: we can't use Dataset.row_counts, because distributed tables are not part of the schema
def get_row_cnt(ds: dlt.Dataset, qualified_table_name: str) -> int:
    return ds.query(f"SELECT COUNT(*) FROM {qualified_table_name}").fetchone()[0]


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["clickhouse_cluster"]),
    ids=lambda x: x.name,
)
def test_replication(destination_config: DestinationTestConfiguration) -> None:
    """Tests typical replication use case of ClickHouse cluster."""

    data = [{"foo": "bar"}]
    replicated = clickhouse_cluster_adapter(
        data,
        table_engine_type="replicated_merge_tree",  # use replicated engine
    ).apply_hints(table_name="replicated")
    not_replicated = clickhouse_cluster_adapter(
        data,
        table_engine_type="merge_tree",  # use non-replicated engine
    ).apply_hints(table_name="not_replicated")
    pipe = destination_config.setup_pipeline("test_replication", dev_mode=True)

    # assert we are connecting to first node on replicated cluster
    assert_clickhouse_cluster_conf(
        config=cast(ClickHouseClusterClientConfiguration, pipe.destination_client().config),
        cluster=REPLICATED_CLUSTER_NAME,
        port=CLICKHOUSE_CLUSTER_NODE_PORTS[0],
        http_port=CLICKHOUSE_CLUSTER_NODE_HTTP_PORTS[0],
    )

    # run pipeline
    load_info = pipe.run([replicated, not_replicated], **destination_config.run_kwargs)
    assert_load_info(load_info)

    # assert table engines
    sql_client = cast(ClickHouseSqlClient, pipe.sql_client())
    assert get_table_engine(sql_client, table_name="replicated") == "ReplicatedMergeTree"
    assert get_table_engine(sql_client, table_name="not_replicated") == "MergeTree"

    # assert row counts on first cluster node
    node_one_ds = pipe.dataset()
    assert len(node_one_ds["replicated"].fetchall()) == 1
    assert len(node_one_ds["not_replicated"].fetchall()) == 1

    # change ports to connect to second cluster node
    set_clickhouse_cluster_conf(
        port=CLICKHOUSE_CLUSTER_NODE_PORTS[1],
        http_port=CLICKHOUSE_CLUSTER_NODE_HTTP_PORTS[1],
    )

    # assert we are connecting to second node now
    assert_clickhouse_cluster_conf(
        config=cast(ClickHouseClusterClientConfiguration, pipe.destination_client().config),
        cluster=REPLICATED_CLUSTER_NAME,
        port=CLICKHOUSE_CLUSTER_NODE_PORTS[1],
        http_port=CLICKHOUSE_CLUSTER_NODE_HTTP_PORTS[1],
    )

    # assert row counts on second cluster node
    node_two_ds = pipe.dataset()
    assert len(node_two_ds["replicated"].fetchall()) == 1  # row was replicated
    assert len(node_two_ds["not_replicated"].fetchall()) == 0  # row was not replicated


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["clickhouse_cluster"]),
    ids=lambda x: x.name,
)
def test_distribution(destination_config: DestinationTestConfiguration) -> None:
    """Tests typical distribution use case of ClickHouse cluster.

    Based on https://clickhouse.com/docs/architecture/horizontal-scaling.

    Use case:
    - cluster: 2 shards, 1 replica each
    - data is stored in sharded table
    - shards are not replicated (ENGINE = MergeTree)
    - distributed table is created on top of sharded table
    - distributed table is used to read from / write into all shards
    """

    # switch to sharded cluster
    set_clickhouse_cluster_conf(cluster=SHARDED_CLUSTER_NAME)

    # define resource and pipeline
    n_rows = 100
    data = [{"foo": "bar"} for _ in range(n_rows)]
    shard_table_name = "sharded_table"
    res = clickhouse_cluster_adapter(
        data,
        table_engine_type="merge_tree",  # use non-replicated engine
        create_distributed_table=True,
    ).apply_hints(table_name=shard_table_name)
    pipe = destination_config.setup_pipeline("test_distribution", dev_mode=True)

    # assert we are connecting to first node on sharded cluster
    assert_clickhouse_cluster_conf(
        config=cast(ClickHouseClusterClientConfiguration, pipe.destination_client().config),
        cluster=SHARDED_CLUSTER_NAME,
        port=CLICKHOUSE_CLUSTER_NODE_PORTS[0],
        http_port=CLICKHOUSE_CLUSTER_NODE_HTTP_PORTS[0],
    )

    # run pipeline
    load_info = pipe.run(res, **destination_config.run_kwargs)
    assert_load_info(load_info)

    # assert table engines
    sql_client = cast(ClickHouseSqlClient, pipe.sql_client())
    dist_table_name = shard_table_name + DEFAULT_DISTRIBUTED_TABLE_SUFFIX
    assert get_table_engine(sql_client, table_name=shard_table_name) == "MergeTree"
    assert get_table_engine(sql_client, table_name=dist_table_name) == "Distributed"

    # define qualified table names
    shard_qual_table_name = sql_client.make_qualified_table_name(shard_table_name)
    dist_qual_table_name = sql_client.make_qualified_table_name(dist_table_name)

    # assert row counts on first cluster node
    node_one_ds = pipe.dataset()
    node_one_shard_row_cnt = get_row_cnt(node_one_ds, shard_qual_table_name)
    node_one_dist_row_cnt = get_row_cnt(node_one_ds, dist_qual_table_name)
    assert node_one_shard_row_cnt < n_rows
    assert node_one_dist_row_cnt == n_rows

    # change ports to connect to second cluster node
    set_clickhouse_cluster_conf(
        port=CLICKHOUSE_CLUSTER_NODE_PORTS[1],
        http_port=CLICKHOUSE_CLUSTER_NODE_HTTP_PORTS[1],
    )

    # assert we are connecting to second node now
    assert_clickhouse_cluster_conf(
        config=cast(ClickHouseClusterClientConfiguration, pipe.destination_client().config),
        cluster=SHARDED_CLUSTER_NAME,
        port=CLICKHOUSE_CLUSTER_NODE_PORTS[1],
        http_port=CLICKHOUSE_CLUSTER_NODE_HTTP_PORTS[1],
    )

    # assert row counts on second cluster node
    node_two_ds = pipe.dataset()
    node_two_shard_row_cnt = get_row_cnt(node_two_ds, shard_qual_table_name)
    node_two_dist_row_cnt = get_row_cnt(node_two_ds, dist_qual_table_name)
    assert node_two_shard_row_cnt < n_rows
    assert node_two_dist_row_cnt == n_rows

    # assert total row count across both shards
    assert node_one_shard_row_cnt + node_two_shard_row_cnt == n_rows


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["clickhouse_cluster"]),
    ids=lambda x: x.name,
)
def test_replication_distribution(destination_config: DestinationTestConfiguration) -> None:
    """Tests typical replication + distribution use case of ClickHouse cluster.

    Based on https://clickhouse.com/docs/architecture/cluster-deployment.

    Use case:
    - cluster: 2 shards, 2 replicas each
    - data is stored in sharded table
    - shards are replicated (ENGINE = ReplicatedMergeTree)
    - distributed table is created on top of sharded table
    - distributed table is used to read from / write into all shards
    """

    # switch to replicated sharded cluster
    set_clickhouse_cluster_conf(cluster=REPLICATED_SHARDED_CLUSTER_NAME)

    # define resource and pipeline
    n_rows = 100
    data = [{"foo": "bar"} for _ in range(n_rows)]
    shard_table_name = "replicated_sharded_table"
    res = clickhouse_cluster_adapter(
        data,
        table_engine_type="replicated_merge_tree",  # use replicated engine
        create_distributed_table=True,
    ).apply_hints(table_name=shard_table_name)
    pipe = destination_config.setup_pipeline("test_replication_distribution", dev_mode=True)

    # assert we are connecting to replicated sharded cluster
    assert_clickhouse_cluster_conf(
        config=cast(ClickHouseClusterClientConfiguration, pipe.destination_client().config),
        cluster=REPLICATED_SHARDED_CLUSTER_NAME,
    )

    # run pipeline
    load_info = pipe.run(res, **destination_config.run_kwargs)
    assert_load_info(load_info)

    # assert table engines
    sql_client = cast(ClickHouseSqlClient, pipe.sql_client())
    dist_table_name = shard_table_name + DEFAULT_DISTRIBUTED_TABLE_SUFFIX
    assert get_table_engine(sql_client, table_name=shard_table_name) == "ReplicatedMergeTree"
    assert get_table_engine(sql_client, table_name=dist_table_name) == "Distributed"

    # define qualified table names
    shard_qual_table_name = sql_client.make_qualified_table_name(shard_table_name)
    dist_qual_table_name = sql_client.make_qualified_table_name(dist_table_name)

    # assert row counts on all cluster nodes
    shard_row_cnts = []
    for node in range(4):
        # set ports to connect to current node
        set_clickhouse_cluster_conf(
            port=CLICKHOUSE_CLUSTER_NODE_PORTS[node],
            http_port=CLICKHOUSE_CLUSTER_NODE_HTTP_PORTS[node],
        )

        # assert we are connecting to correct node
        assert_clickhouse_cluster_conf(
            config=cast(ClickHouseClusterClientConfiguration, pipe.destination_client().config),
            port=CLICKHOUSE_CLUSTER_NODE_PORTS[node],
            http_port=CLICKHOUSE_CLUSTER_NODE_HTTP_PORTS[node],
        )

        # assert row counts on current node
        node_ds = pipe.dataset()
        node_shard_row_cnt = get_row_cnt(node_ds, shard_qual_table_name)
        node_dist_row_cnt = get_row_cnt(node_ds, dist_qual_table_name)
        assert node_shard_row_cnt < n_rows
        assert node_dist_row_cnt == n_rows

        # save shard row count for cross-node assertions
        shard_row_cnts.append(node_shard_row_cnt)

    # replicated shards should have same row counts
    assert shard_row_cnts[0] == shard_row_cnts[1]  # shard 1 replicas on nodes 1,2
    assert shard_row_cnts[2] == shard_row_cnts[3]  # shard 2 replicas on nodes 3,4

    # assert total row count across both shards
    assert shard_row_cnts[0] + shard_row_cnts[2] == n_rows  # shard 1 on node 1, shard 2 on node 3
    assert shard_row_cnts[1] + shard_row_cnts[3] == n_rows  # shard 1 on node 2, shard 2 on node 4
