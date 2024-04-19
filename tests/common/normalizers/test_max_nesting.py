from typing import Any, Dict, List

import dlt
import pytest

from dlt.common import json
from pytest_mock.plugin import MockerFixture

from tests.common.utils import json_case_path


TOP_LEVEL_TABLES = ["bot_events"]

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
        (0, 1, TOP_LEVEL_TABLES),
        (1, 1, TOP_LEVEL_TABLES),
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
        destination="duckdb",
        full_refresh=True,
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
    rasa_event_bot_metadata: Dict[str, Any], mocker: MockerFixture
):
    @dlt.resource(max_table_nesting=1)
    def rasa_bot_events_with_nesting_lvl_one():
        yield rasa_event_bot_metadata

    @dlt.resource(max_table_nesting=2)
    def rasa_bot_events_with_nesting_lvl_two():
        yield rasa_event_bot_metadata

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
            }
        ]

    assert "x-normalizer" in rasa_bot_events_with_nesting_lvl_one._hints
    assert "x-normalizer" in rasa_bot_events_with_nesting_lvl_two._hints
    assert "x-normalizer" not in third_resource_with_nested_data._hints

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
        destination="duckdb",
        full_refresh=True,
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
    assert len(tables) == 4
    assert tables == [
        "third_resource_with_nested_data",
        "third_resource_with_nested_data__payload__hints",
        "third_resource_with_nested_data__payload__hints__f_float",
        "third_resource_with_nested_data__payload__hints__f_float__comments",
    ]
