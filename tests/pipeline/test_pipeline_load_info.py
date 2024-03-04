from typing import Dict, List

import dlt


def get_metrics_table(tables: List[Dict]) -> List[str]:
    return [
        table_name for table_name in tables if table_name["name"].startswith("_load_info__metrics")
    ]


def test_pipeline_load_info_metrics_is_always_a_single_table() -> None:
    data = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
    ]

    pipeline = dlt.pipeline(
        pipeline_name="quick_start",
        destination="duckdb",
        dataset_name="mydata",
    )

    load_info = pipeline.run(data, table_name="users")

    pipeline.run([load_info], table_name="_load_info")
    metrics_tables_first_run = get_metrics_table(pipeline.default_schema.data_tables())

    load_info = pipeline.run(data, table_name="users")
    pipeline.run([load_info], table_name="_load_info")
    metrics_tables_second_run = get_metrics_table(pipeline.default_schema.data_tables())

    assert len(metrics_tables_first_run) == len(metrics_tables_second_run)
    assert metrics_tables_first_run == metrics_tables_second_run
