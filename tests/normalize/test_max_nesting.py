from typing import Any, Dict, List

import dlt
import pytest

from dlt.common import json
from dlt.destinations import dummy
from tests.common.utils import json_case_path


ROOT_TABLES = ["bot_events"]

ALL_TABLES_FOR_RASA_EVENT = [
    "bot_events",
    "bot_events__metadata__known_recipients",
    "bot_events__metadata__transaction_history__spend__target",
    "bot_events__metadata__transaction_history__spend__starbucks",
    "bot_events__metadata__transaction_history__spend__amazon",
    "bot_events__metadata__transaction_history__deposit__employer",
    "bot_events__metadata__transaction_history__deposit__interest",
    "bot_events__metadata__vendor_list",
]

ALL_TABLES_FOR_RASA_EVENT_NESTING_LEVEL_2 = [
    "bot_events",
    "bot_events__metadata__known_recipients",
    "bot_events__metadata__vendor_list",
]


@pytest.fixture(scope="module")
def rasa_event_bot_metadata():
    with open(json_case_path("rasa_event_bot_metadata"), "rb") as f:
        return json.load(f)


@pytest.mark.parametrize(
    "nesting_level,expected_num_tables,expected_table_names",
    (
        (0, 1, ROOT_TABLES),
        (1, 1, ROOT_TABLES),
        (2, 3, ALL_TABLES_FOR_RASA_EVENT_NESTING_LEVEL_2),
        (5, 8, ALL_TABLES_FOR_RASA_EVENT),
        (15, 8, ALL_TABLES_FOR_RASA_EVENT),
        (25, 8, ALL_TABLES_FOR_RASA_EVENT),
        (1000, 8, ALL_TABLES_FOR_RASA_EVENT),
    ),
)
def test_resource_max_nesting(
    nesting_level: int,
    expected_num_tables: int,
    expected_table_names: List[str],
    rasa_event_bot_metadata: Dict[str, Any],
):
    @dlt.resource(max_table_nesting=nesting_level)
    def bot_events():
        yield rasa_event_bot_metadata

    assert "x-normalizer" in bot_events._hints

    pipeline_name = f"test_max_table_nesting_{nesting_level}_{expected_num_tables}"
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=dummy(timeout=0.1, completed_prob=1),
        dev_mode=True,
    )

    pipeline.run(bot_events)
    assert pipeline.schemas.keys()
    assert pipeline_name in pipeline.schema_names

    pipeline_schema = pipeline.schemas[pipeline_name]
    assert len(pipeline_schema.data_table_names()) == expected_num_tables

    all_table_names = pipeline_schema.data_table_names()
    for table_name in expected_table_names:
        assert table_name in all_table_names


def test_with_multiple_resources_with_max_table_nesting_levels(
    rasa_event_bot_metadata: Dict[str, Any],
):
    """Test max_table_nesting feature with multiple resources and a source
    Test scenario includes

    1. Testing three different sources with set and unset `max_table_nesting` parameter
        and checks if the number of created tables in the schema match the expected numbers
        and the exact list table names have been collected;
    2. For the same parent source we change the `max_table_nesting` and verify if it is respected
        by the third resource `third_resource_with_nested_data` as well as checking
        the number of created tables in the current schema;
    3. Run combined test where we set `max_table_nesting` for the parent source and check
        if this `max_table_nesting` is respected by child resources where they don't define their
        own nesting level;
    4. Run the pipeline with set `max_table_nesting` of a resource then override it and
        rerun the pipeline to check if the number and names of tables are expected;
    5. Create source and resource both with defined `max_nesting_level` and check if we respect
        `max_nesting_level` from resource;
    """

    @dlt.resource(max_table_nesting=1)
    def rasa_bot_events_with_nesting_lvl_one():
        yield rasa_event_bot_metadata

    @dlt.resource(max_table_nesting=2)
    def rasa_bot_events_with_nesting_lvl_two():
        yield rasa_event_bot_metadata

    all_table_names_for_third_resource = [
        "third_resource_with_nested_data",
        "third_resource_with_nested_data__payload__hints",
        "third_resource_with_nested_data__payload__hints__f_float",
        "third_resource_with_nested_data__payload__hints__f_float__comments",
        "third_resource_with_nested_data__params",
    ]

    @dlt.resource
    def third_resource_with_nested_data():  # first top level table `third_resource_with_nested_data`
        yield [
            {
                "id": 1,
                "payload": {
                    "f_int": 7817289713,
                    "f_float": 878172.8292,
                    "f_timestamp": "2024-04-19T11:40:32.901899+00:00",
                    "f_bool": False,
                    "hints": [  # second table `third_resource_with_nested_data__payload__hints`
                        {
                            "f_bool": "bool",
                            "f_timestamp": "bigint",
                            "f_float": [  # third table `third_resource_with_nested_data__payload__hints__f_float`
                                {
                                    "cond": "precision > 4",
                                    "then": "decimal",
                                    "else": "float",
                                    "comments": [  # fourth table `third_resource_with_nested_data__payload__hints__f_float__comments`
                                        {
                                            "text": "blabla bla bla we promise magix",
                                            "author": "bart",
                                        }
                                    ],
                                }
                            ],
                        }
                    ],
                },
                "params": [{"id": 1, "q": "search"}, {"id": 2, "q": "hashtag-search"}],
            }
        ]

    assert "x-normalizer" in rasa_bot_events_with_nesting_lvl_one._hints
    assert "x-normalizer" in rasa_bot_events_with_nesting_lvl_two._hints
    assert rasa_bot_events_with_nesting_lvl_one.max_table_nesting == 1
    assert rasa_bot_events_with_nesting_lvl_two.max_table_nesting == 2
    assert rasa_bot_events_with_nesting_lvl_one._hints["x-normalizer"]["max_nesting"] == 1  # type: ignore[typeddict-item]
    assert rasa_bot_events_with_nesting_lvl_two._hints["x-normalizer"]["max_nesting"] == 2  # type: ignore[typeddict-item]
    assert "x-normalizer" not in third_resource_with_nested_data._hints

    # Check scenario #1
    @dlt.source(max_table_nesting=100)
    def some_data():
        return [
            rasa_bot_events_with_nesting_lvl_one(),
            rasa_bot_events_with_nesting_lvl_two(),
            third_resource_with_nested_data(),
        ]

    pipeline_name = "test_different_table_nesting_levels"
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=dummy(timeout=0.1, completed_prob=1),
        dev_mode=True,
    )

    pipeline.run(some_data(), write_disposition="append")
    pipeline_schema = pipeline.schemas[pipeline.default_schema_name]
    all_table_names = pipeline_schema.data_table_names()

    # expect only one table for resource `rasa_bot_events_with_nesting_lvl_one`
    tables = [tbl for tbl in all_table_names if tbl.endswith("nesting_lvl_one")]
    assert len(tables) == 1
    assert tables == ["rasa_bot_events_with_nesting_lvl_one"]

    # expect three tables for resource `rasa_bot_events_with_nesting_lvl_two`
    tables = [tbl for tbl in all_table_names if "nesting_lvl_two" in tbl]
    assert len(tables) == 3
    assert tables == [
        "rasa_bot_events_with_nesting_lvl_two",
        "rasa_bot_events_with_nesting_lvl_two__metadata__known_recipients",
        "rasa_bot_events_with_nesting_lvl_two__metadata__vendor_list",
    ]

    # expect four tables for resource `third_resource_with_nested_data`
    tables = [tbl for tbl in all_table_names if "third_resource" in tbl]
    assert len(tables) == 5
    assert tables == all_table_names_for_third_resource

    # Check scenario #2
    # now we need to check `third_resource_with_nested_data`
    # using different nesting levels at the source level
    # First we do with max_table_nesting=0
    @dlt.source(max_table_nesting=0)
    def some_data_v2():
        yield third_resource_with_nested_data()

    pipeline.drop()
    pipeline.run(some_data_v2(), write_disposition="append")
    pipeline_schema = pipeline.schemas[pipeline.default_schema_name]
    all_table_names = pipeline_schema.data_table_names()
    assert len(all_table_names) == 1
    assert all_table_names == [
        "third_resource_with_nested_data",
    ]

    # Second we do with max_table_nesting=1
    some_data_source = some_data_v2()
    some_data_source.max_table_nesting = 1

    pipeline.drop()
    pipeline.run(some_data_source, write_disposition="append")
    pipeline_schema = pipeline.schemas[pipeline.default_schema_name]
    all_table_names = pipeline_schema.data_table_names()
    assert len(all_table_names) == 2
    assert all_table_names == [
        "third_resource_with_nested_data",
        "third_resource_with_nested_data__params",
    ]

    # Second we do with max_table_nesting=2
    some_data_source = some_data_v2()
    some_data_source.max_table_nesting = 3

    pipeline.drop()
    pipeline.run(some_data_source, write_disposition="append")
    pipeline_schema = pipeline.schemas[pipeline.default_schema_name]
    all_table_names = pipeline_schema.data_table_names()

    # 5 because payload is a dictionary not a collection of dictionaries
    assert len(all_table_names) == 5
    assert all_table_names == all_table_names_for_third_resource

    # Check scenario #3
    pipeline.drop()
    some_data_source = some_data()
    some_data_source.max_table_nesting = 0
    pipeline.run(some_data_source, write_disposition="append")
    pipeline_schema = pipeline.schemas[pipeline.default_schema_name]
    all_table_names = pipeline_schema.data_table_names()
    assert len(all_table_names) == 5
    assert sorted(all_table_names) == [
        "rasa_bot_events_with_nesting_lvl_one",
        "rasa_bot_events_with_nesting_lvl_two",
        "rasa_bot_events_with_nesting_lvl_two__metadata__known_recipients",
        "rasa_bot_events_with_nesting_lvl_two__metadata__vendor_list",
        "third_resource_with_nested_data",
    ]

    # Check scenario #4
    # Set max_table_nesting via the setter and check the tables
    pipeline.drop()
    rasa_bot_events_resource = rasa_bot_events_with_nesting_lvl_one()
    pipeline.run(
        rasa_bot_events_resource,
        dataset_name="bot_events",
        write_disposition="append",
    )
    pipeline_schema = pipeline.schemas[pipeline.default_schema_name]
    all_table_names = pipeline_schema.data_table_names()
    count_all_tables_first_run = len(all_table_names)
    tables = pipeline_schema.data_table_names()
    assert count_all_tables_first_run == 1
    assert tables == ["rasa_bot_events_with_nesting_lvl_one"]

    # now adjust the max_table_nesting for resource and check
    pipeline.drop()
    rasa_bot_events_resource.max_table_nesting = 2
    assert rasa_bot_events_resource.max_table_nesting == 2
    pipeline.run(
        rasa_bot_events_resource,
        dataset_name="bot_events",
        write_disposition="append",
    )
    all_table_names = pipeline_schema.data_table_names()
    count_all_tables_second_run = len(all_table_names)
    assert count_all_tables_first_run < count_all_tables_second_run

    tables = pipeline_schema.data_table_names()
    assert count_all_tables_second_run == 3
    assert tables == [
        "rasa_bot_events_with_nesting_lvl_one",
        "rasa_bot_events_with_nesting_lvl_one__metadata__known_recipients",
        "rasa_bot_events_with_nesting_lvl_one__metadata__vendor_list",
    ]

    pipeline.drop()
    rasa_bot_events_resource.max_table_nesting = 10
    assert rasa_bot_events_resource.max_table_nesting == 10
    pipeline.run(rasa_bot_events_resource, dataset_name="bot_events")
    all_table_names = pipeline_schema.data_table_names()
    count_all_tables_second_run = len(all_table_names)
    assert count_all_tables_first_run < count_all_tables_second_run

    tables = pipeline_schema.data_table_names()
    assert count_all_tables_second_run == 8
    assert tables == [
        "rasa_bot_events_with_nesting_lvl_one",
        "rasa_bot_events_with_nesting_lvl_one__metadata__known_recipients",
        "rasa_bot_events_with_nesting_lvl_one__metadata__transaction_history__spend__target",
        "rasa_bot_events_with_nesting_lvl_one__metadata__transaction_history__spend__starbucks",
        "rasa_bot_events_with_nesting_lvl_one__metadata__transaction_history__spend__amazon",
        "rasa_bot_events_with_nesting_lvl_one__metadata__transaction_history__deposit__employer",
        "rasa_bot_events_with_nesting_lvl_one__metadata__transaction_history__deposit__interest",
        "rasa_bot_events_with_nesting_lvl_one__metadata__vendor_list",
    ]

    pipeline.drop()
    third_resource_with_nested_data.max_table_nesting = 10
    assert third_resource_with_nested_data.max_table_nesting == 10
    pipeline.run(third_resource_with_nested_data)
    all_table_names = pipeline_schema.data_table_names()
    count_all_tables_second_run = len(all_table_names)
    assert count_all_tables_first_run < count_all_tables_second_run

    tables_with_nesting_level_set = pipeline_schema.data_table_names()
    assert count_all_tables_second_run == 5
    assert tables_with_nesting_level_set == all_table_names_for_third_resource

    # Set max_table_nesting=None and check if the same tables exist
    third_resource_with_nested_data.max_table_nesting = None
    assert third_resource_with_nested_data.max_table_nesting is None
    pipeline.run(third_resource_with_nested_data)
    all_table_names = pipeline_schema.data_table_names()
    count_all_tables_second_run = len(all_table_names)
    assert count_all_tables_first_run < count_all_tables_second_run

    tables = pipeline_schema.data_table_names()
    assert count_all_tables_second_run == 5
    assert tables == all_table_names_for_third_resource
    assert tables == tables_with_nesting_level_set

    # Check scenario #5
    # We give priority `max_table_nesting` of the resource if it is defined
    @dlt.source(max_table_nesting=1000)
    def some_data_with_table_nesting():
        yield rasa_bot_events_with_nesting_lvl_one()

    pipeline.drop()
    pipeline.run(some_data_with_table_nesting())
    pipeline_schema = pipeline.schemas[pipeline.default_schema_name]
    tables = pipeline_schema.data_table_names()
    assert len(tables) == 1
    assert tables == ["rasa_bot_events_with_nesting_lvl_one"]

    # Now check the case when `max_table_nesting` is not defined in the resource
    rasa_bot_events_with_nesting_lvl_one.max_table_nesting = None

    pipeline.drop()
    pipeline.run(some_data_with_table_nesting())
    pipeline_schema = pipeline.schemas[pipeline.default_schema_name]
    tables = pipeline_schema.data_table_names()
    assert len(tables) == 8
    assert tables == [
        "rasa_bot_events_with_nesting_lvl_one",
        "rasa_bot_events_with_nesting_lvl_one__metadata__known_recipients",
        "rasa_bot_events_with_nesting_lvl_one__metadata__transaction_history__spend__target",
        "rasa_bot_events_with_nesting_lvl_one__metadata__transaction_history__spend__starbucks",
        "rasa_bot_events_with_nesting_lvl_one__metadata__transaction_history__spend__amazon",
        "rasa_bot_events_with_nesting_lvl_one__metadata__transaction_history__deposit__employer",
        "rasa_bot_events_with_nesting_lvl_one__metadata__transaction_history__deposit__interest",
        "rasa_bot_events_with_nesting_lvl_one__metadata__vendor_list",
    ]
