import dlt
from dlt.destinations.adapters import clickhouse_adapter
from tests.pipeline.utils import assert_load_info


def test_clickhouse_adapter() -> None:
    @dlt.resource
    def merge_tree_resource():
        yield {"field1": 1, "field2": 2}

    @dlt.resource
    def replicated_merge_tree_resource():
        yield {"field1": 1, "field2": 2}

    @dlt.resource
    def not_annotated_resource():
        yield {"field1": 1, "field2": 2}

    clickhouse_adapter(merge_tree_resource, table_engine_type="merge_tree")
    clickhouse_adapter(replicated_merge_tree_resource, table_engine_type="replicated_merge_tree")

    pipe = dlt.pipeline(pipeline_name="adapter_test", destination="clickhouse", full_refresh=True)
    pack = pipe.run([merge_tree_resource, replicated_merge_tree_resource, not_annotated_resource])

    assert_load_info(pack)

    with pipe.sql_client() as client:
        # get map of table names to full table names
        tables = {}
        for table in client._list_tables():
            if "resource" in table:
                tables[table.split("___")[1]] = table
        assert (len(tables.keys())) == 3

        # check content
        for full_table_name in tables.values():
            with client.execute_query(f"SELECT * FROM {full_table_name};") as cursor:
                res = cursor.fetchall()
                assert tuple(res[0])[:2] == (1, 2)

        # check table format
        # fails now, because we do not have a cluster (I think), it will fall back to SharedMergeTree
        for full_table_name in tables.values():
            with client.execute_query(
                "SELECT database, name, engine, engine_full FROM system.tables WHERE name ="
                f" '{full_table_name}';"
            ) as cursor:
                res = cursor.fetchall()
                # this should test that two tables should be replicatedmergetree tables
                assert tuple(res[0])[2] == "SharedMergeTree"

    # we can check the gen table sql though
    with pipe.destination_client() as dest_client:
        for table in tables.keys():
            sql = dest_client._get_table_update_sql(  # type: ignore[attr-defined]
                table, pipe.default_schema.tables[table]["columns"].values(), generate_alter=False
            )
            if table == "merge_tree_resource":
                assert "ENGINE = MergeTree" in sql[0]
            else:
                assert "ENGINE = ReplicatedMergeTree" in sql[0]
