import os
import time
import pytest
from typing import cast
from unittest.mock import patch

import dlt
from dlt.destinations.adapters import clickhouse_cluster_adapter
from dlt.destinations.exceptions import DatabaseTransientException, DestinationConnectionError
from dlt.destinations.impl.clickhouse.sql_client import ClickHouseSqlClient
from dlt.destinations.impl.clickhouse.typing import TTableEngineType
from dlt.destinations.impl.clickhouse_cluster.configuration import (
    DEFAULT_DISTRIBUTED_TABLE_SUFFIX,
    ClickHouseClusterClientConfiguration,
    ClickHouseClusterCredentials,
)
from dlt.destinations.impl.clickhouse_cluster.sql_client import ClickHouseClusterSqlClient
from dlt.extract import decorators
from tests.load.clickhouse_cluster.utils import (
    CLICKHOUSE_CLUSTER_HOST,
    REPLICATED_CLUSTER_NAME,
    REPLICATED_SHARDED_CLUSTER_NAME,
    SHARDED_CLUSTER_NAME,
    assert_clickhouse_cluster_conf,
    clickhouse_cluster_node_paused,
    get_node_name,
    get_node_port,
    get_table_engine,
    set_clickhouse_cluster_conf,
)
from tests.load.utils import DestinationTestConfiguration, destinations_configs
from tests.pipeline.utils import assert_load_info


# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


# NOTE: we can't use Dataset.row_counts, because distributed tables are not part of the schema
def get_row_cnt(ds: dlt.Dataset, qualified_table_name: str) -> int:
    return ds.query(f"SELECT COUNT(*) FROM {qualified_table_name}").fetchone()[0]


# patch ClickHouseClusterCredentials to remove insert_quorum setting this test, because we intentionally
# shut down nodes and insert_quorum = 'auto' would prevent inserts when a node is down (cluster has
# only two nodes, so quorum cannot be achieved when one is down)
@patch.object(
    ClickHouseClusterCredentials,
    "__session_settings__",
    {
        k: v
        for k, v in ClickHouseClusterCredentials.__session_settings__.items()
        if k != "insert_quorum"
    },
)
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, cid="clickhouse-cluster-default"),
    ids=lambda x: x.name,
)
def test_alt_hosts(destination_config: DestinationTestConfiguration) -> None:
    res = decorators.resource([{"foo": "bar"}], name="foo")

    # define cluster config
    cluster = REPLICATED_CLUSTER_NAME  # 1 shard, 2 replicas
    table_engine_type: TTableEngineType = "replicated_merge_tree"
    port = get_node_port(1)
    http_port = get_node_port(1, http=True)
    alt_hosts = f"{CLICKHOUSE_CLUSTER_HOST}:{get_node_port(2)}"
    alt_http_hosts = f"{CLICKHOUSE_CLUSTER_HOST}:{get_node_port(2, http=True)}"

    set_clickhouse_cluster_conf(
        cluster=cluster,
        table_engine_type=table_engine_type,
        port=port,
        http_port=http_port,
        alt_hosts=alt_hosts,
        alt_http_hosts=alt_http_hosts,
    )
    # lower timeout for faster test execution
    os.environ["DESTINATION__CLICKHOUSE_CLUSTER__CREDENTIALS__SEND_RECEIVE_TIMEOUT"] = "2"

    pipe = destination_config.setup_pipeline("test_alt_hosts", dev_mode=True)

    assert_clickhouse_cluster_conf(
        config=cast(ClickHouseClusterClientConfiguration, pipe.destination_client().config),
        cluster=cluster,
        table_engine_type=table_engine_type,
        port=port,
        http_port=http_port,
        alt_hosts=alt_hosts,
        alt_http_hosts=alt_http_hosts,
    )

    sql_client = cast(ClickHouseClusterSqlClient, pipe.sql_client())

    # by default, we connect to first replica
    assert get_node_name(sql_client, driver="clickhouse_driver") == "01"
    assert get_node_name(sql_client, driver="clickhouse_connect") == "01"

    # run pipeline as integration test
    load_info = pipe.run(res, **destination_config.run_kwargs)
    assert_load_info(load_info)

    # assert row count on first node
    node_one_ds = pipe.dataset()
    assert get_node_name(node_one_ds.sql_client, driver="clickhouse_driver") == "01"  # type: ignore[arg-type]
    assert len(node_one_ds["foo"].fetchall()) == 1

    # pause first node to test failover
    with clickhouse_cluster_node_paused(1):
        # now we should connect to second replica
        assert get_node_name(sql_client, driver="clickhouse_driver") == "02"
        assert get_node_name(sql_client, driver="clickhouse_connect") == "02"

        # assert row count on second node, before second load
        time.sleep(1)  # give cluster extra time to replicate data to from first to second node
        node_two_ds = pipe.dataset()
        assert get_node_name(node_two_ds.sql_client, driver="clickhouse_driver") == "02"  # type: ignore[arg-type]
        assert len(node_two_ds["foo"].fetchall()) == 1

        # run pipeline as integration test
        load_info = pipe.run(res, **destination_config.run_kwargs)
        assert_load_info(load_info)

        # assert row count on second node, after second load
        node_two_ds = pipe.dataset()
        assert get_node_name(node_two_ds.sql_client, driver="clickhouse_driver") == "02"  # type: ignore[arg-type]
        assert len(node_two_ds["foo"].fetchall()) == 2

    # after unpausing first node we should connect to it again
    assert get_node_name(sql_client, driver="clickhouse_driver") == "01"
    assert get_node_name(sql_client, driver="clickhouse_connect") == "01"

    # assert row count on first node, after second load
    time.sleep(1)  # give cluster extra time to replicate data from second to first node
    node_one_ds = pipe.dataset()
    assert get_node_name(node_one_ds.sql_client, driver="clickhouse_driver") == "01"  # type: ignore[arg-type]
    assert len(node_one_ds["foo"].fetchall()) == 2  # second load was replicated after unpausing

    # pause both nodes and assert connection attempts fail
    with clickhouse_cluster_node_paused(1, 2):
        # clickhouse_driver driver
        with pytest.raises(DatabaseTransientException):
            with sql_client:
                sql_client.execute_sql("SELECT 1;")

        # clickhouse_connect driver
        with pytest.raises(DestinationConnectionError):
            sql_client.clickhouse_connect_client()


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        # we override configuration in test with adapter; limit to default config here to avoid test redundancy
        cid="clickhouse-cluster-default",
    ),
    ids=lambda x: x.name,
)
def test_replication(destination_config: DestinationTestConfiguration) -> None:
    """Tests typical replication use case of ClickHouse cluster.

    Based on https://clickhouse.com/docs/architecture/replication.

    Use case:
    - cluster: 1 shard, 2 replicas
    - shard is replicated (ENGINE = ReplicatedMergeTree)
    """

    # switch to replicated cluster
    set_clickhouse_cluster_conf(cluster=REPLICATED_CLUSTER_NAME)

    data = [{"foo": "bar"}]
    replicated = decorators.resource(data, name="replicated")
    replicated = clickhouse_cluster_adapter(replicated, table_engine_type="replicated_merge_tree")
    not_replicated = decorators.resource(data, name="not_replicated")
    not_replicated = clickhouse_cluster_adapter(not_replicated, table_engine_type="merge_tree")
    pipe = destination_config.setup_pipeline("test_replication", dev_mode=True)

    # assert we are connecting to first node on replicated cluster
    assert_clickhouse_cluster_conf(
        config=cast(ClickHouseClusterClientConfiguration, pipe.destination_client().config),
        cluster=REPLICATED_CLUSTER_NAME,
        port=get_node_port(1),
        http_port=get_node_port(1, http=True),
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
        port=get_node_port(2),
        http_port=get_node_port(2, http=True),
    )

    # assert we are connecting to second node now
    assert_clickhouse_cluster_conf(
        config=cast(ClickHouseClusterClientConfiguration, pipe.destination_client().config),
        cluster=REPLICATED_CLUSTER_NAME,
        port=get_node_port(2),
        http_port=get_node_port(2, http=True),
    )

    # assert row counts on second cluster node
    node_two_ds = pipe.dataset()
    assert len(node_two_ds["replicated"].fetchall()) == 1  # row was replicated
    assert len(node_two_ds["not_replicated"].fetchall()) == 0  # row was not replicated


@pytest.mark.parametrize(
    "destination_config",
    # we use one staging and one non-staging config; they use different insert methods, and we
    # want to assert both write into the distributed table instead of the local table
    destinations_configs(
        default_sql_configs=True,
        default_staging_configs=True,
        cid=("clickhouse-cluster-default", "clickhouse-cluster-s3-staging"),
    ),
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
        create_distributed_tables=True,
    ).apply_hints(table_name=shard_table_name)
    pipe = destination_config.setup_pipeline("test_distribution", dev_mode=True)

    # assert we are connecting to first node on sharded cluster
    assert_clickhouse_cluster_conf(
        config=cast(ClickHouseClusterClientConfiguration, pipe.destination_client().config),
        cluster=SHARDED_CLUSTER_NAME,
        port=get_node_port(1),
        http_port=get_node_port(1, http=True),
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
        port=get_node_port(2),
        http_port=get_node_port(2, http=True),
    )

    # assert we are connecting to second node now
    assert_clickhouse_cluster_conf(
        config=cast(ClickHouseClusterClientConfiguration, pipe.destination_client().config),
        cluster=SHARDED_CLUSTER_NAME,
        port=get_node_port(2),
        http_port=get_node_port(2, http=True),
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
    destinations_configs(
        default_sql_configs=True,
        cid="clickhouse-cluster-replicated-distributed",
    ),
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
    res = decorators.resource(data, name=shard_table_name)
    res = clickhouse_cluster_adapter(res, create_distributed_tables=True)
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
    time.sleep(1)  # give cluster extra time to replicate data
    shard_row_cnts = []
    for node in (1, 2, 3, 4):
        # set ports to connect to current node
        set_clickhouse_cluster_conf(
            port=get_node_port(node),
            http_port=get_node_port(node, http=True),
        )

        # assert we are connecting to correct node
        assert_clickhouse_cluster_conf(
            config=cast(ClickHouseClusterClientConfiguration, pipe.destination_client().config),
            port=get_node_port(node),
            http_port=get_node_port(node, http=True),
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
