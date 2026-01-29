from typing import Optional, cast
import pytest

from dlt.destinations.adapters import clickhouse_cluster_adapter
from dlt.destinations.impl.clickhouse_cluster.clickhouse_cluster import ClickHouseClusterClient
from dlt.destinations.impl.clickhouse_cluster.clickhouse_cluster_adapter import (
    CREATE_DISTRIBUTED_TABLES_HINT,
    DISTRIBUTED_TABLE_SUFFIX_HINT,
    SHARDING_KEY_HINT,
)
from dlt.destinations.impl.clickhouse_cluster.configuration import (
    DEFAULT_DISTRIBUTED_TABLE_SUFFIX,
    DEFAULT_SHARDING_KEY,
)
from tests.load.clickhouse_cluster.utils import (
    CLICKHOUSE_CLUSTER_DATABASE,
    SHARDED_CLUSTER_NAME,
    assert_clickhouse_cluster_conf,
    clickhouse_cluster_database_created,
    client,
    get_table_engine,
    set_clickhouse_cluster_conf,
    table_exists,
)
from dlt.destinations.impl.clickhouse.typing import (
    CODEC_HINT,
    PARTITION_HINT,
    SETTINGS_HINT,
    SORT_HINT,
    TABLE_ENGINE_TYPE_HINT,
    TABLE_ENGINE_TYPE_TO_CLICKHOUSE_ATTR,
)
from tests.load.utils import DestinationTestConfiguration, destinations_configs
from tests.pipeline.utils import assert_load_info

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


def test_clickhouse_cluster_adapter_forwarding():
    """Asserts `clickhouse_cluster_adapter` forwards params to `clickhouse_adapter` correctly."""

    res = clickhouse_cluster_adapter(
        [{"foo": "bar"}],
        # include params available in `clickhouse_adapter`
        table_engine_type="replicated_merge_tree",
        sort=["foo"],
        partition="toYYYYMMDD(foo)",
        settings={"max_suspicious_broken_parts": 500},
        codecs={"foo": "LZ4HC"},
        # exclude params specific to `clickhouse_cluster_adapter`
    )
    table_schema = res.compute_table_schema()
    assert table_schema[TABLE_ENGINE_TYPE_HINT] == "replicated_merge_tree"  # type: ignore[typeddict-item]
    assert table_schema[SORT_HINT] == ["foo"]  # type: ignore[typeddict-item]
    assert table_schema[PARTITION_HINT] == "toYYYYMMDD(foo)"  # type: ignore[typeddict-item]
    assert table_schema[SETTINGS_HINT] == {"max_suspicious_broken_parts": 500}  # type: ignore[typeddict-item]
    assert table_schema["columns"]["foo"][CODEC_HINT] == "LZ4HC"  # type: ignore[typeddict-item]


def test_clickhouse_cluster_adapter_defaults(client: ClickHouseClusterClient) -> None:
    # first test that adapter does not set default table hint values in schema

    # adapt resource without providing arguments
    res = clickhouse_cluster_adapter([{"foo": "bar"}])

    # assert hints are not set
    table_schema = res.compute_table_schema()
    assert CREATE_DISTRIBUTED_TABLES_HINT not in table_schema
    assert DISTRIBUTED_TABLE_SUFFIX_HINT not in table_schema
    assert SHARDING_KEY_HINT not in table_schema

    # now test that default values are loaded from ClickHouseClusterClientConfiguration at runtime

    table_name = "event_test_table"

    # non-custom default values
    assert client.config.create_distributed_tables is False
    assert client.config.distributed_table_suffix == DEFAULT_DISTRIBUTED_TABLE_SUFFIX
    assert client.config.sharding_key == DEFAULT_SHARDING_KEY
    prepared_table = client.prepare_load_table(table_name)
    assert prepared_table[CREATE_DISTRIBUTED_TABLES_HINT] is False  # type: ignore[typeddict-item]
    assert prepared_table[DISTRIBUTED_TABLE_SUFFIX_HINT] == DEFAULT_DISTRIBUTED_TABLE_SUFFIX  # type: ignore[typeddict-item]
    assert prepared_table[SHARDING_KEY_HINT] == DEFAULT_SHARDING_KEY  # type: ignore[typeddict-item]

    # custom default values
    client.config.create_distributed_tables = True
    client.config.distributed_table_suffix = "_custom_suffix"
    client.config.sharding_key = "custom sharding key"  # invalid key value, but okay for unit test
    prepared_table = client.prepare_load_table(table_name)
    assert prepared_table[CREATE_DISTRIBUTED_TABLES_HINT] is True  # type: ignore[typeddict-item]
    assert prepared_table[DISTRIBUTED_TABLE_SUFFIX_HINT] == "_custom_suffix"  # type: ignore[typeddict-item]
    assert prepared_table[SHARDING_KEY_HINT] == "custom sharding key"  # type: ignore[typeddict-item]


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, cid="clickhouse-cluster-default"),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize(
    "distributed_tables_database, distributed_table_suffix, sharding_key",
    [
        pytest.param(None, None, None, id="defaults"),
        pytest.param(None, None, "user_id + 1", id="custom-sharding-key"),
        pytest.param(None, "_custom_suffix", None, id="custom-distributed-table-suffix"),
        pytest.param("dlt_data_dist", None, None, id="custom-distributed-tables-database"),
    ],
)
def test_clickhouse_cluster_adapter_distributed_table(
    destination_config: DestinationTestConfiguration,
    distributed_tables_database: Optional[str],
    distributed_table_suffix: Optional[str],
    sharding_key: Optional[str],
) -> None:
    set_clickhouse_cluster_conf(
        cluster=SHARDED_CLUSTER_NAME, distributed_tables_database=distributed_tables_database
    )

    # define pipeline and clients
    pipe = destination_config.setup_pipeline(
        "test_clickhouse_cluster_adapter_distributed_table", dev_mode=True
    )
    client = cast(ClickHouseClusterClient, pipe.destination_client())
    sql_client = client.sql_client

    assert_clickhouse_cluster_conf(
        client.config,
        cluster=SHARDED_CLUSTER_NAME,
        distributed_tables_database=distributed_tables_database,
    )

    # define table names
    shard_table_name = "sharded_table"
    effective_dist_table_suffix = distributed_table_suffix or DEFAULT_DISTRIBUTED_TABLE_SUFFIX
    dist_table_name = shard_table_name + effective_dist_table_suffix
    shard_qual_table_name = sql_client.make_qualified_table_name(shard_table_name)
    dist_db = sql_client.distributed_tables_database_name
    with sql_client.with_alternative_database_name(dist_db):
        dist_qual_table_name = sql_client.make_qualified_table_name(dist_table_name)

    # assert distributed table is created in correct database
    if distributed_tables_database:
        assert dist_qual_table_name.startswith(f"`{distributed_tables_database}`.")
    else:
        assert dist_qual_table_name.startswith(f"`{CLICKHOUSE_CLUSTER_DATABASE}`.")

    # create resource with distributed table hints
    data = [
        {"UserID": 100},
        # include nested data to test distributed tables for child/grandchild tables
        {"UserID": 101, "child": [{"grandchild": [1, 2]}]},
    ]
    res = clickhouse_cluster_adapter(
        data,
        create_distributed_tables=True,
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
    assert table_schema[CREATE_DISTRIBUTED_TABLES_HINT] is True  # type: ignore[typeddict-item]
    assert table_schema.get(DISTRIBUTED_TABLE_SUFFIX_HINT) == distributed_table_suffix

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
    engine = TABLE_ENGINE_TYPE_TO_CLICKHOUSE_ATTR[client.config.table_engine_type]
    assert f"ENGINE = {engine}" in shard_stmt

    # distributed table statement
    dist_stmt = stmts[1]
    database, table = sql_client.make_qualified_table_name(shard_table_name, quote=False).split(".")
    effective_sharding_key = sharding_key or DEFAULT_SHARDING_KEY
    expected_engine = (
        f"Distributed('{SHARDED_CLUSTER_NAME}', '{database}', '{table}', {effective_sharding_key})"
    )
    expected_dist_stmt = (
        f"CREATE OR REPLACE TABLE {dist_qual_table_name} ON CLUSTER {SHARDED_CLUSTER_NAME} AS"
        f" {shard_qual_table_name} ENGINE = {expected_engine};"
    )
    assert dist_stmt == expected_dist_stmt

    # assert distributed tables get created and have correct engine
    with clickhouse_cluster_database_created(sql_client, dist_db):
        load_info = pipe.run(res, **destination_config.run_kwargs)
        assert_load_info(load_info)

        # parent table
        assert table_exists(sql_client, dist_table_name, dist_db)
        actual_engine = get_table_engine(
            sql_client,
            dist_table_name,
            full=True,
            alternative_database_name=dist_db,
        )
        assert actual_engine == expected_engine

        # child table
        child_shard_table_name = shard_table_name + "__child"
        child_dist_table_name = child_shard_table_name + effective_dist_table_suffix
        assert table_exists(sql_client, child_dist_table_name, dist_db)
        child_actual_engine = get_table_engine(
            sql_client,
            child_dist_table_name,
            full=True,
            alternative_database_name=dist_db,
        )
        database, table = sql_client.make_qualified_table_name(
            child_shard_table_name, quote=False
        ).split(".")
        child_expected_engine = (  # child tables always use default sharding key
            f"Distributed('{SHARDED_CLUSTER_NAME}', '{database}', '{table}',"
            f" {DEFAULT_SHARDING_KEY})"
        )
        assert child_actual_engine == child_expected_engine

        # grandchild table
        grandchild_shard_table_name = shard_table_name + "__child__grandchild"
        grandchild_dist_table_name = grandchild_shard_table_name + effective_dist_table_suffix
        assert table_exists(sql_client, grandchild_dist_table_name, dist_db)
        grandchild_actual_engine = get_table_engine(
            sql_client,
            grandchild_dist_table_name,
            full=True,
            alternative_database_name=dist_db,
        )
        database, table = sql_client.make_qualified_table_name(
            grandchild_shard_table_name, quote=False
        ).split(".")
        grandchild_expected_engine = (  # (grand)child tables always use default sharding key
            f"Distributed('{SHARDED_CLUSTER_NAME}', '{database}', '{table}',"
            f" {DEFAULT_SHARDING_KEY})"
        )
        assert grandchild_actual_engine == grandchild_expected_engine

        # `drop_dataset` drops distributed tables in addition to standard tables
        with sql_client:
            sql_client.drop_dataset()
        # distributed tables
        assert not table_exists(sql_client, dist_table_name, dist_db)
        assert not table_exists(sql_client, child_dist_table_name, dist_db)
        assert not table_exists(sql_client, grandchild_dist_table_name, dist_db)
        # standard tables
        assert not table_exists(sql_client, shard_table_name)
        assert not table_exists(sql_client, child_shard_table_name)
        assert not table_exists(sql_client, grandchild_shard_table_name)
