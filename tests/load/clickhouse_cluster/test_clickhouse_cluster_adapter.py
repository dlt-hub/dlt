import pytest

from dlt.common.schema import Schema
from dlt.common.utils import uniq_id
from dlt.destinations import clickhouse_cluster
from dlt.destinations.adapters import clickhouse_cluster_adapter
from dlt.destinations.impl.clickhouse_cluster.clickhouse_cluster import ClickHouseClusterClient
from dlt.destinations.impl.clickhouse_cluster.clickhouse_cluster_adapter import (
    CREATE_DISTRIBUTED_TABLE_HINT,
    DEFAULT_DISTRIBUTED_TABLE_SUFFIX,
    DISTRIBUTED_TABLE_SUFFIX_HINT,
)
from dlt.destinations.impl.clickhouse_cluster.configuration import (
    ClickHouseClusterClientConfiguration,
)
from tests.load.clickhouse_cluster.utils import SHARDED_CLUSTER_NAME

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.fixture
def client(empty_schema: Schema) -> ClickHouseClusterClient:
    # return client without opening connection
    return clickhouse_cluster().client(
        empty_schema,
        ClickHouseClusterClientConfiguration()._bind_dataset_name(dataset_name="test_" + uniq_id()),
    )


@pytest.mark.parametrize(
    "distributed_table_suffix", (DEFAULT_DISTRIBUTED_TABLE_SUFFIX, "_custom_suffix")
)
def test_clickhouse_cluster_adapter_distributed_table(
    client: ClickHouseClusterClient, distributed_table_suffix: str
) -> None:
    # switch to sharded cluster
    client.config.cluster = SHARDED_CLUSTER_NAME

    # define table names
    shard_table_name = "sharded_table"
    dist_table_name = shard_table_name + distributed_table_suffix
    shard_qual_table_name = client.sql_client.make_qualified_table_name(shard_table_name)
    dist_qual_table_name = client.sql_client.make_qualified_table_name(dist_table_name)

    # create resource with distributed table hints
    res = clickhouse_cluster_adapter(
        [{"foo": "bar"}],
        create_distributed_table=True,
        distributed_table_suffix=distributed_table_suffix,
    ).apply_hints(
        table_name=shard_table_name,
        columns={"foo": {"data_type": "text"}},
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
    shard_stmt = stmts[0]
    dist_stmt = stmts[1]
    assert shard_stmt.startswith(
        f"CREATE TABLE {shard_qual_table_name} ON CLUSTER {SHARDED_CLUSTER_NAME} ("
    )
    assert "ENGINE = MergeTree" in shard_stmt
    assert dist_stmt.startswith(
        f"CREATE TABLE {dist_qual_table_name} ON CLUSTER {SHARDED_CLUSTER_NAME} ENGINE ="
        " Distributed("
    )
