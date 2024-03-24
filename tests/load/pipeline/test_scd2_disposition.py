import pytest, dlt, os, pendulum

from tests.load.pipeline.utils import destinations_configs, DestinationTestConfiguration
from tests.load.pipeline.utils import (
    load_tables_to_dicts,
)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["duckdb"]),
    ids=lambda x: x.name,
)
def test_simple_scd2_load(destination_config: DestinationTestConfiguration) -> None:
    # use scd2
    first_load = pendulum.now()
    os.environ["DESTINATION__MERGE_STRATEGY"] = "scd2"
    os.environ["DESTINATION__LOAD_TIMESTAMP"] = first_load.to_iso8601_string()

    @dlt.resource(name="items", write_disposition="merge")
    def load_items():
        yield from [
            {
                "id": 1,
                "name": "one",
            },
            {
                "id": 2,
                "name": "two",
                "children": [
                    {
                        "id_of": 2,
                        "name": "child2",
                    }
                ],
            },
            {
                "id": 3,
                "name": "three",
                "children": [
                    {
                        "id_of": 3,
                        "name": "child3",
                    }
                ],
            },
        ]

    p = destination_config.setup_pipeline("test", full_refresh=True)
    p.run(load_items())

    # new version of item 1
    # item 3 deleted
    # item 2 has a new child
    @dlt.resource(name="items", write_disposition="merge")
    def load_items_2():
        yield from [
            {
                "id": 1,
                "name": "one_new",
            },
            {
                "id": 2,
                "name": "two",
                "children": [
                    {
                        "id_of": 2,
                        "name": "child2_new",
                    }
                ],
            },
        ]

    second_load = pendulum.now()
    os.environ["DESTINATION__LOAD_TIMESTAMP"] = second_load.to_iso8601_string()

    p.run(load_items_2())

    tables = load_tables_to_dicts(p, "items", "items__children")
    # we should have 4 items in total (3 from the first load and an update of item 1 from the second load)
    assert len(tables["items"]) == 4
    # 2 should be active (1 and 2)
    active_items = [item for item in tables["items"] if item["_dlt_valid_until"] is None]
    inactive_items = [item for item in tables["items"] if item["_dlt_valid_until"] is not None]
    active_items.sort(key=lambda i: i["id"])
    inactive_items.sort(key=lambda i: i["id"])
    assert len(active_items) == 2

    # changed in the second load
    assert active_items[0]["id"] == 1
    assert active_items[0]["name"] == "one_new"
    assert active_items[0]["_dlt_valid_from"] == second_load

    # did not change in the second load
    assert active_items[1]["id"] == 2
    assert active_items[1]["name"] == "two"
    assert active_items[1]["_dlt_valid_from"] == first_load

    # was valid between first and second load
    assert inactive_items[0]["id"] == 1
    assert inactive_items[0]["name"] == "one"
    assert inactive_items[0]["_dlt_valid_from"] == first_load
    assert inactive_items[0]["_dlt_valid_until"] == second_load

    # was valid between first and second load
    assert inactive_items[1]["id"] == 3
    assert inactive_items[1]["name"] == "three"
    assert inactive_items[1]["_dlt_valid_from"] == first_load
    assert inactive_items[1]["_dlt_valid_until"] == second_load

    # child tables
    assert len(tables["items__children"]) == 3
    active_child_items = [
        item for item in tables["items__children"] if item["_dlt_valid_until"] is None
    ]
    inactive_child_items = [
        item for item in tables["items__children"] if item["_dlt_valid_until"] is not None
    ]
    active_child_items.sort(key=lambda i: i["id_of"])
    inactive_child_items.sort(key=lambda i: i["id_of"])

    assert len(active_child_items) == 1

    # the one active child item should be linked to the right parent, was create during 2. load
    assert active_child_items[0]["id_of"] == 2
    assert active_child_items[0]["name"] == "child2_new"
    assert active_child_items[0]["_dlt_parent_id"] == active_items[1]["_dlt_id"]
    assert active_child_items[0]["_dlt_valid_from"] == second_load

    # check inactive child items
    assert inactive_child_items[0]["id_of"] == 2
    assert inactive_child_items[0]["name"] == "child2"
    assert inactive_child_items[0]["_dlt_parent_id"] == active_items[1]["_dlt_id"]
    assert inactive_child_items[0]["_dlt_valid_from"] == first_load
    assert inactive_child_items[0]["_dlt_valid_until"] == second_load

    assert inactive_child_items[1]["id_of"] == 3
    assert inactive_child_items[1]["name"] == "child3"
    assert inactive_child_items[1]["_dlt_parent_id"] == inactive_items[1]["_dlt_id"]
    assert inactive_child_items[1]["_dlt_valid_from"] == first_load
    assert inactive_child_items[1]["_dlt_valid_until"] == second_load
