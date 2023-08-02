import dlt, os, pytest

from tests.pipeline.utils import  assert_load_info
from tests.load.pipeline.utils import  load_table_counts, load_tables_to_dicts
from tests.load.pipeline.utils import destinations_configs, DestinationTestConfiguration

REPLACE_STRATEGIES = ["truncate-and-insert", "insert-from-staging", "staging-optimized"]

@pytest.mark.parametrize("destination_config", destinations_configs(local_filesystem_configs=True, default_staging_configs=True, default_non_staging_configs=True), ids=lambda x: x.name)
@pytest.mark.parametrize("replace_strategy", REPLACE_STRATEGIES)
def test_replace_disposition(destination_config: DestinationTestConfiguration, replace_strategy: str) -> None:

    # only allow 40 items per file
    os.environ['DATA_WRITER__FILE_MAX_ITEMS'] = "40"
    # use staging tables for replace
    os.environ['DESTINATION__REPLACE_STRATEGY'] = replace_strategy

    pipeline = destination_config.setup_pipeline("test_replace_strategies")

    global offset
    offset = 1000

    # keep merge key with unknown column to test replace SQL generator
    @dlt.resource(name="items", write_disposition="replace", primary_key="id", merge_key="NA")
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
    info = pipeline.run(load_items, loader_file_format=destination_config.file_format)

    # we should have all items loaded
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])

    assert table_counts["items"] == 120
    assert table_counts["items__sub_items"] == 240
    assert table_counts["items__sub_items__sub_sub_items"] == 120

    # second run with higher offset so we can check the results
    offset = 1000
    info = pipeline.run(load_items, loader_file_format=destination_config.file_format)
    assert_load_info(info)

    # we should have all items loaded
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])

    assert table_counts["items"] == 120
    assert table_counts["items__sub_items"] == 240
    assert table_counts["items__sub_items__sub_sub_items"] == 120

    # check we really have the replaced data in our destination
    table_dicts = load_tables_to_dicts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert {x for i,x in enumerate(range(1000, 1120), 1)} == {int(x["id"]) for x in table_dicts["items"]}
    assert {x for i,x in enumerate(range(2000, 2000+120), 1)}.union({x for i,x in enumerate(range(3000, 3000+120), 1)}) == {int(x["id"]) for x in table_dicts["items__sub_items"]}
    assert {x for i,x in enumerate(range(4000, 4120), 1)} == {int(x["id"]) for x in table_dicts["items__sub_items__sub_sub_items"]}


    # we need to test that destination tables including child tables are cleared when we yield none from the resource
    @dlt.resource(name="items", write_disposition="replace", primary_key="id")
    def load_items_none():
        yield
    info = pipeline.run(load_items_none, loader_file_format=destination_config.file_format)

    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])

    # table and child tables should be cleared
    assert table_counts["items"] == 0
    assert table_counts.get("items__sub_items", 0) == 0
    assert table_counts.get("items__sub_items__sub_sub_items", 0) == 0


@pytest.mark.parametrize("destination_config", destinations_configs(local_filesystem_configs=True, default_staging_configs=True, default_non_staging_configs=True), ids=lambda x: x.name)
@pytest.mark.parametrize("replace_strategy", REPLACE_STRATEGIES)
def test_replace_table_clearing(destination_config: DestinationTestConfiguration,replace_strategy: str) -> None:

    # use staging tables for replace
    os.environ['DESTINATION__REPLACE_STRATEGY'] = replace_strategy

    pipeline = destination_config.setup_pipeline("test_replace_table_clearing")

    @dlt.resource(name="main_resource", write_disposition="replace", primary_key="id")
    def items_with_subitems():
        data = {
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
        yield dlt.mark.with_table_name(data, "items")
        yield dlt.mark.with_table_name(data, "other_items")

    @dlt.resource(name="main_resource", write_disposition="replace", primary_key="id")
    def items_without_subitems():
        data = [{
            "id": 1,
            "name": "item",
            "sub_items": []
        }]
        yield dlt.mark.with_table_name(data, "items")
        yield dlt.mark.with_table_name(data, "other_items")

    @dlt.resource(name="main_resource", write_disposition="replace", primary_key="id")
    def items_with_subitems_yield_none():
        yield None
        yield None
        data = [{
            "id": 1,
            "name": "item",
            "sub_items": [{
                "id": 101,
                "name": "sub item 101"
            },{
                "id": 101,
                "name": "sub item 102"
            }]
        }]
        yield dlt.mark.with_table_name(data, "items")
        yield dlt.mark.with_table_name(data, "other_items")
        yield None

    # this resource only gets loaded once, and should remain populated regardless of the loads to the other tables
    @dlt.resource(name="static_items", write_disposition="replace", primary_key="id")
    def static_items():
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

    @dlt.resource(name="main_resource", write_disposition="replace", primary_key="id")
    def yield_none():
        yield

    # regular call
    pipeline.run([items_with_subitems, static_items], loader_file_format=destination_config.file_format)
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert table_counts["items"] == 1
    assert table_counts["items__sub_items"] == 2
    assert table_counts["other_items"] == 1
    assert table_counts["other_items__sub_items"] == 2
    assert table_counts["static_items"] == 1
    assert table_counts["static_items__sub_items"] == 2

    # see if child table gets cleared
    pipeline.run(items_without_subitems, loader_file_format=destination_config.file_format)
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert table_counts["items"] == 1
    assert table_counts.get("items__sub_items", 0) == 0
    assert table_counts["other_items"] == 1
    assert table_counts.get("other_items__sub_items", 0) == 0
    assert table_counts["static_items"] == 1
    assert table_counts["static_items__sub_items"] == 2

    # see if yield none clears everything
    pipeline.run(items_with_subitems, loader_file_format=destination_config.file_format)
    pipeline.run(yield_none, loader_file_format=destination_config.file_format)
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert table_counts.get("items", 0) == 0
    assert table_counts.get("items__sub_items", 0) == 0
    assert table_counts.get("other_items", 0) == 0
    assert table_counts.get("other_items__sub_items", 0) == 0
    assert table_counts["static_items"] == 1
    assert table_counts["static_items__sub_items"] == 2

    # see if yielding something next to other none entries still goes into db
    pipeline.run(items_with_subitems_yield_none, loader_file_format=destination_config.file_format)
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert table_counts["items"] == 1
    assert table_counts["items__sub_items"] == 2
    assert table_counts["other_items"] == 1
    assert table_counts["other_items__sub_items"] == 2
    assert table_counts["static_items"] == 1
    assert table_counts["static_items__sub_items"] == 2
