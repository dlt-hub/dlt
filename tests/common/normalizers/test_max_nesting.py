from typing import List

import dlt
import pytest

from dlt.common import json
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
    nesting_level: int, expected_num_tables: int, expected_table_names: List[str]
):
    @dlt.resource(max_table_nesting=nesting_level)
    def bot_events():
        print("nesting_level", nesting_level)
        with open(json_case_path("rasa_event_bot_metadata"), "rb") as f:
            yield json.load(f)

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
