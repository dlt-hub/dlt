import json
import dlt

from tests.common.utils import json_case_path


data = [
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"},
]


def test_pipeline_load_info_metrics_schema_is_not_chaning() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="quick_start",
        destination="duckdb",
        dataset_name="mydata",
    )

    load_info = pipeline.run(data, table_name="users")

    pipeline.run([load_info], table_name="_load_info")
    first_version_hash = pipeline.default_schema.version_hash

    load_info = pipeline.run(data, table_name="users")
    pipeline.run([load_info], table_name="_load_info")
    second_version_hash = pipeline.default_schema.version_hash

    assert (
        len(
            {
                first_version_hash,
                second_version_hash,
            }
        )
        == 1
    )

    event_data = json.load(open(json_case_path("rasa_event_bot_metadata"), "rb"))
    load_info = pipeline.run([event_data], table_name="event_data")
    pipeline.run([load_info], table_name="_load_info")
    third_version_hash = pipeline.default_schema.version_hash

    event_data = json.load(open(json_case_path("rasa_event_bot_metadata"), "rb"))
    load_info = pipeline.run([event_data], table_name="event_data")
    pipeline.run([load_info], table_name="_load_info")
    fourth_version_hash = pipeline.default_schema.version_hash

    assert (
        len(
            {
                third_version_hash,
                fourth_version_hash,
            }
        )
        == 1
    )
