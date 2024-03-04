import dlt


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

    assert first_version_hash == second_version_hash
