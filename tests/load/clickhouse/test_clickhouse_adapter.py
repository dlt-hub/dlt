import pytest

from typing import Generator, Dict, cast

import dlt
from dlt.destinations.adapters import clickhouse_adapter
from dlt.destinations.impl.clickhouse.clickhouse import ClickHouseClient
from dlt.destinations.impl.clickhouse.sql_client import ClickHouseSqlClient
from dlt.destinations.impl.clickhouse.typing import PARTITION_HINT, TDeployment
from tests.load.clickhouse.utils import clickhouse_client, get_deployment_type
from tests.pipeline.utils import assert_load_info


def test_clickhouse_adapter() -> None:
    @dlt.resource
    def merge_tree_resource() -> Generator[Dict[str, int], None, None]:
        yield {"field1": 1, "field2": 2}

    # `ReplicatedMergeTree` has been supplanted by `ReplacingMergeTree` on CH Cloud,
    # which is automatically selected even if `MergeTree` is selected.
    # See https://clickhouse.com/docs/en/cloud/reference/shared-merge-tree.

    # The `Log` Family of engines are only supported in self-managed deployments.
    # So can't test in CH Cloud CI.

    @dlt.resource
    def replicated_merge_tree_resource() -> Generator[Dict[str, int], None, None]:
        yield {"field1": 1, "field2": 2}

    @dlt.resource
    def not_annotated_resource() -> Generator[Dict[str, int], None, None]:
        """Non annotated resource will default to `SharedMergeTree` for CH cloud
        and `MergeTree` for self-managed installation."""
        yield {"field1": 1, "field2": 2}

    clickhouse_adapter(merge_tree_resource, table_engine_type="merge_tree")
    clickhouse_adapter(replicated_merge_tree_resource, table_engine_type="replicated_merge_tree")

    pipe = dlt.pipeline(
        pipeline_name="adapter_test",
        destination="clickhouse",
        dev_mode=True,
        dataset_name="adapter_test_ds",
    )

    with pipe.sql_client() as client:
        deployment_type: TDeployment = get_deployment_type(cast(ClickHouseSqlClient, client))

    if deployment_type == "ClickHouseCloud":
        pack = pipe.run(
            [
                merge_tree_resource,
                replicated_merge_tree_resource,
                not_annotated_resource,
            ]
        )
    else:
        # `ReplicatedMergeTree` not supported if only a single node.
        pack = pipe.run([merge_tree_resource, not_annotated_resource])

    assert_load_info(pack)

    with pipe.sql_client() as client:
        # Get a map of table names to full table names.
        tables = {}
        for table in client._list_tables():
            if "resource" in table:
                tables[table.split("___")[1]] = table
        if deployment_type == "ClickHouseCloud":
            assert (len(tables.keys())) == 3
        else:
            assert (len(tables.keys())) == 2

        # Check the table content.
        for full_table_name in tables.values():
            with client.execute_query(f"SELECT * FROM {full_table_name};") as cursor:
                res = cursor.fetchall()
                assert tuple(res[0])[:2] == (1, 2)

        # Check the table engine.
        for table_name, full_table_name in tables.items():
            with client.execute_query(
                "SELECT database, name, engine, engine_full FROM system.tables "
                f"WHERE name = '{full_table_name}';"
            ) as cursor:
                res = cursor.fetchall()
                if table_name in (
                    "merge_tree_resource",
                    "replicated_merge_tree_resource",
                ):
                    if deployment_type == "ClickHouseCloud":
                        assert tuple(res[0])[2] in (
                            "MergeTree",
                            "SharedMergeTree",
                            "ReplicatedMergeTree",
                        )
                    else:
                        assert tuple(res[0])[2] in ("MergeTree",)
                else:
                    # Non annotated resource needs to default to detected installation
                    # type, i.e. cloud or self-managed.
                    # CI runs on CH cloud, so will be `SharedMergeTree`.
                    if deployment_type == "ClickHouseCloud":
                        assert tuple(res[0])[2] == "SharedMergeTree"
                    else:
                        assert tuple(res[0])[2] == "MergeTree"

    # We can check the generated table's SQL, though.
    with pipe.destination_client() as dest_client:
        for table in tables:
            sql = dest_client._get_table_update_sql(  # type: ignore[attr-defined]
                table,
                pipe.default_schema.tables[table]["columns"].values(),
                generate_alter=False,
            )
            if table == "replicated_merge_tree_resource":
                assert "ENGINE = ReplicatedMergeTree" in sql[0]
            else:
                assert "ENGINE = MergeTree" or "ENGINE = SharedMergeTree" in sql[0]


def test_clickhouse_adapter_partition(clickhouse_client: ClickHouseClient) -> None:
    table_name = "partitioned"

    @dlt.resource(table_name=table_name)
    def data():
        yield [{"timestamp": "2025-12-15T13:32:45Z", "user_id": 1}]

    # partition hint gets set correctly
    partition = "toYYYYMMDD(timestamp)"
    res = clickhouse_adapter(data, partition=partition)
    table_schema = res.compute_table_schema()
    assert table_schema[PARTITION_HINT] == partition  # type: ignore[typeddict-item]

    # partition clause gets set correctly
    clickhouse_client.schema.update_table(table_schema)
    new_columns = list(table_schema["columns"].values())
    stmts = clickhouse_client._get_table_update_sql(table_name, new_columns, False)
    assert len(stmts) == 1
    sql = stmts[0]
    assert f"PARTITION BY {partition}" in sql

    # % character gets escaped in SQL:
    unescaped_partition = "sipHash64(user_id) % 16"  # single %
    escaped_partition = "sipHash64(user_id) %% 16"  # double %%
    res = clickhouse_adapter(data, partition=unescaped_partition)
    table_schema = res.compute_table_schema()
    assert table_schema[PARTITION_HINT] == unescaped_partition  # type: ignore[typeddict-item]
    clickhouse_client.schema.update_table(table_schema)
    new_columns = list(table_schema["columns"].values())
    stmts = clickhouse_client._get_table_update_sql(table_name, new_columns, False)
    assert len(stmts) == 1
    sql = stmts[0]
    assert f"PARTITION BY {escaped_partition}" in sql

    # raises if `partition` is not a string
    with pytest.raises(TypeError):
        clickhouse_adapter(data, partition=True)  # type: ignore[arg-type]
