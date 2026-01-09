from typing import cast
import pytest

from dlt.destinations.adapters import clickhouse_cluster_adapter
from dlt.destinations.impl.clickhouse_cluster.clickhouse_cluster import ClickHouseClusterClient
from dlt.destinations.impl.clickhouse_cluster.clickhouse_cluster_adapter import (
    CREATE_DISTRIBUTED_TABLE_HINT,
    DEFAULT_DISTRIBUTED_TABLE_SUFFIX,
    DEFAULT_SHARDING_KEY,
    DISTRIBUTED_TABLE_SUFFIX_HINT,
)
from dlt.destinations.impl.clickhouse_cluster.sql_client import ClickHouseClusterSqlClient
from tests.load.clickhouse_cluster.utils import (
    SHARDED_CLUSTER_NAME,
    get_table_engine,
    set_clickhouse_cluster_conf,
)
from tests.load.utils import DestinationTestConfiguration, destinations_configs
from tests.pipeline.utils import assert_load_info

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["clickhouse_cluster"]),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize(
    "distributed_table_suffix, sharding_key",
    [
        pytest.param(DEFAULT_DISTRIBUTED_TABLE_SUFFIX, DEFAULT_SHARDING_KEY, id="defaults"),
        pytest.param(DEFAULT_DISTRIBUTED_TABLE_SUFFIX, "user_id + 1", id="custom-sharding-key"),
        pytest.param("_custom_suffix", DEFAULT_SHARDING_KEY, id="custom-distributed-table-suffix"),
    ],
)
def test_clickhouse_cluster_adapter_distributed_table(
    destination_config: DestinationTestConfiguration,
    distributed_table_suffix: str,
    sharding_key: str,
) -> None:
    # switch to sharded cluster
    set_clickhouse_cluster_conf(cluster=SHARDED_CLUSTER_NAME)

    # define pipeline and client
    pipe = destination_config.setup_pipeline(
        "test_clickhouse_cluster_adapter_distributed_table", dev_mode=True
    )
    client = cast(ClickHouseClusterClient, pipe.destination_client())

    # define table names
    shard_table_name = "sharded_table"
    dist_table_name = shard_table_name + distributed_table_suffix
    shard_qual_table_name = client.sql_client.make_qualified_table_name(shard_table_name)
    dist_qual_table_name = client.sql_client.make_qualified_table_name(dist_table_name)

    # create resource with distributed table hints
    res = clickhouse_cluster_adapter(
        [{"UserID": 100}, {"UserID": 101}],
        create_distributed_table=True,
        distributed_table_suffix=distributed_table_suffix,
        sharding_key=sharding_key,
    ).apply_hints(
        table_name=shard_table_name,
        columns={
            "UserID": {
                "data_type": "bigint",
                "nullable": False,  # columns part of sharding key must not be nullable
            }
        },
    )

    # assert hints are set correctly
    table_schema = res.compute_table_schema()
    assert table_schema[CREATE_DISTRIBUTED_TABLE_HINT] is True  # type: ignore[typeddict-item]
    assert table_schema[DISTRIBUTED_TABLE_SUFFIX_HINT] == distributed_table_suffix  # type: ignore[typeddict-item]

    # assert create table statements are generated correctly
    client.schema.update_table(table_schema)
    new_columns = list(table_schema["columns"].values())
    stmts = client._get_table_update_sql(shard_table_name, new_columns, False)
    assert len(stmts) == 2  # additional statement for distributed table

    # sharded table statement
    shard_stmt = stmts[0]
    assert shard_stmt.startswith(
        f"CREATE TABLE {shard_qual_table_name} ON CLUSTER {SHARDED_CLUSTER_NAME} ("
    )
    assert "ENGINE = MergeTree" in shard_stmt

    # distributed table statement
    dist_stmt = stmts[1]
    sql_client = cast(ClickHouseClusterSqlClient, client.sql_client)
    database, table = sql_client.make_qualified_table_name(shard_table_name, quote=False).split(".")
    expected_engine = (
        f"Distributed('{SHARDED_CLUSTER_NAME}', '{database}', '{table}', {sharding_key})"
    )
    expected_dist_stmt = (
        f"CREATE TABLE {dist_qual_table_name} ON CLUSTER {SHARDED_CLUSTER_NAME} AS"
        f" {shard_qual_table_name} ENGINE = {expected_engine};"
    )
    assert dist_stmt == expected_dist_stmt

    # assert distributed table gets created and has correct engine
    load_info = pipe.run(res, **destination_config.run_kwargs)
    assert_load_info(load_info)
    actual_engine = get_table_engine(sql_client, dist_table_name, full=True)
    assert actual_engine == expected_engine
