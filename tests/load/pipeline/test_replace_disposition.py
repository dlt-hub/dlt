import dlt, os, pytest

from tests.pipeline.utils import  assert_load_info
from tests.load.pipeline.utils import  load_table_counts
from tests.utils import ALL_DESTINATIONS

@pytest.mark.parametrize("destination", ALL_DESTINATIONS)
def test_replace_disposition(destination: str) -> None:
    # only allow 40 items per file
    os.environ['DATA_WRITER__FILE_MAX_ITEMS'] = "40"
    pipeline = dlt.pipeline(pipeline_name='test_replace_strategies', destination=destination, dataset_name='test_replace_strategies', full_refresh=True)

    global offset
    offset = 1000

    @dlt.resource(table_name="items", write_disposition="replace", primary_key="id")
    def load_items():
        # will produce 3 jobs for the main table with 40 items each
        # 6 jobs for the sub_items
        # 3 jobs for the sub_sub_items
        global offset
        for _, index in enumerate(range(offset, offset+120), 1):
            yield {
                "id": index,
                "name": f"item {index}",
                "sub_items": [{
                    "id": index + 1000,
                    "name": f"sub item {index + 1000}"
                },{
                    "id": index + 2000,
                    "name": f"sub item {index + 2000}",
                    "sub_sub_items": [{
                        "id": index + 3000,
                        "name": f"sub item {index + 3000}",
                    }]
                }]
                }


    # first run with offset 0
    info = pipeline.run(load_items)
    # second run with higher offset so we can check the results
    offset = 1000
    info = pipeline.run(load_items)
    assert_load_info(info)

    # check we really have the replaced data in our destination
    with pipeline._get_destination_client(pipeline.default_schema) as client:
        rows = client.sql_client.execute_sql("SELECT id FROM items")
        assert {x for i,x in enumerate(range(1000, 1120), 1)} == {x[0] for x in rows}
        rows = client.sql_client.execute_sql("SELECT id FROM items__sub_items")
        assert {x for i,x in enumerate(range(2000, 2000+120), 1)}.union({x for i,x in enumerate(range(3000, 3000+120), 1)}) == {x[0] for x in rows}
        rows = client.sql_client.execute_sql("SELECT id FROM items__sub_items__sub_sub_items")
        assert {x for i,x in enumerate(range(4000, 4120), 1)} == {x[0] for x in rows}

    # as it stands, only one job will be present for each table and it is unclear which of the items have made it into the table
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert table_counts["items"] == 120
    assert table_counts["items__sub_items"] == 240
    assert table_counts["items__sub_items__sub_sub_items"] == 120

    # we need to test that destination tables including child tables are cleared when we yield none from the resource
    @dlt.resource(table_name="items", write_disposition="replace", primary_key="id")
    def load_items_none():
        yield None
    info = pipeline.run(load_items_none)

    # as it stands, only one job will be present for each table and it is unclear which of the items have made it into the table
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])

    # table and child tables should be cleared
    assert table_counts["items"] == 0
    assert table_counts["items__sub_items"] == 0
    assert table_counts["items__sub_items__sub_sub_items"] == 0


@pytest.mark.parametrize("destination", ALL_DESTINATIONS)
def test_replace_table_clearing(destination: str) -> None:

    pipeline = dlt.pipeline(pipeline_name='test_replace_table_clearing', destination=destination, dataset_name='test_replace_table_clearing', full_refresh=True)


    @dlt.resource(table_name="items", write_disposition="replace", primary_key="id")
    def items_with_subitems():
        yield {
            "id": 1,
            "name": "item",
            "sub_items": [{
                "id": 101,
                "name": "sub item 101"
            },{
                "id": 101,
                "name": "sub item 102"
            }]
        }

    @dlt.resource(table_name="items", write_disposition="replace", primary_key="id")
    def items_without_subitems():
        yield {
            "id": 1,
            "name": "item",
            "sub_items": []
        }

    @dlt.resource(table_name="items", write_disposition="replace", primary_key="id")
    def items_with_subitems_yield_none():
        yield None
        yield None
        yield {
            "id": 1,
            "name": "item",
            "sub_items": [{
                "id": 101,
                "name": "sub item 101"
            },{
                "id": 101,
                "name": "sub item 102"
            }]
        }
        yield None

    @dlt.resource(table_name="items", write_disposition="replace", primary_key="id")
    def yield_none():
        yield None

    # regular call
    pipeline.run(items_with_subitems)
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert table_counts["items"] == 1
    assert table_counts["items__sub_items"] == 2

    # see if child table gets cleared
    pipeline.run(items_without_subitems)
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert table_counts["items"] == 1
    assert table_counts["items__sub_items"] == 0

    # see if yield none clears everything
    pipeline.run(items_with_subitems)
    pipeline.run(yield_none)
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert table_counts["items"] == 0
    assert table_counts["items__sub_items"] == 0

    # see if yielding something next to other none entries still goes into db
    pipeline.run(items_with_subitems_yield_none)
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert table_counts["items"] == 1
    assert table_counts["items__sub_items"] == 2