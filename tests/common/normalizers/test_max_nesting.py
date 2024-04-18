import dlt

from dlt.common import json
from tests.common.utils import json_case_path


def test_resource_max_nesting():
    @dlt.resource(max_table_nesting=1)
    def bot_events():
        with open(json_case_path("rasa_event_bot_metadata"), "rb") as f:
            yield json.load(f)

    pipeline = dlt.pipeline(
        pipeline_name="test_max_table_nesting",
        destination="duckdb",
    )

    pipeline.run(bot_events, write_disposition="append")
