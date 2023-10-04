import pytest
from typing import Dict, Any, List

import dlt, os
from dlt.common import json, sleep
from copy import deepcopy
from dlt.common.utils import uniq_id
from dlt.common.schema.typing import TDataType

from tests.load.pipeline.test_merge_disposition import github
from tests.load.pipeline.utils import  load_table_counts
from tests.pipeline.utils import  assert_load_info
from tests.load.utils import TABLE_ROW_ALL_DATA_TYPES, TABLE_UPDATE_COLUMNS_SCHEMA, assert_all_data_types_row
from tests.cases import table_update_and_row
from tests.load.pipeline.utils import destinations_configs, DestinationTestConfiguration


@dlt.resource(table_name="issues", write_disposition="merge", primary_key="id", merge_key=("node_id", "url"))
def load_modified_issues():
    with open("tests/normalize/cases/github.issues.load_page_5_duck.json", "r", encoding="utf-8") as f:
        issues = json.load(f)

        # change 2 issues
        issue = next(filter(lambda i: i["id"] == 1232152492, issues))
        issue["number"] = 105

        issue = next(filter(lambda i: i["id"] == 1142699354, issues))
        issue["number"] = 300

        yield from issues


@pytest.mark.parametrize("destination_config", destinations_configs(all_staging_configs=True), ids=lambda x: x.name)
def test_staging_load(destination_config: DestinationTestConfiguration) -> None:

    pipeline = destination_config.setup_pipeline(pipeline_name='test_stage_loading_5', dataset_name="test_staging_load" + uniq_id())

    info = pipeline.run(github(), loader_file_format=destination_config.file_format)
    assert_load_info(info)
    package_info = pipeline.get_load_package_info(info.loads_ids[0])
    assert package_info.state == "loaded"

    assert len(package_info.jobs["failed_jobs"]) == 0
    # we have 4 parquet and 4 reference jobs plus one merge job
    num_jobs = 4 + 4 + 1 if destination_config.supports_merge else 4 + 4
    assert len(package_info.jobs["completed_jobs"]) == num_jobs
    assert len([x for x in package_info.jobs["completed_jobs"] if x.job_file_info.file_format == "reference"]) == 4
    assert len([x for x in package_info.jobs["completed_jobs"] if x.job_file_info.file_format == destination_config.file_format]) == 4
    if destination_config.supports_merge:
        assert len([x for x in package_info.jobs["completed_jobs"] if x.job_file_info.file_format == "sql"]) == 1

    initial_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert initial_counts["issues"] == 100

    # check item of first row in db
    with pipeline.sql_client() as sql_client:
        rows = sql_client.execute_sql("SELECT url FROM issues WHERE id = 388089021 LIMIT 1")
        assert rows[0][0] == "https://api.github.com/repos/duckdb/duckdb/issues/71"

    if destination_config.supports_merge:
        # test merging in some changed values
        info = pipeline.run(load_modified_issues, loader_file_format=destination_config.file_format)
        assert_load_info(info)
        assert pipeline.default_schema.tables["issues"]["write_disposition"] == "merge"
        merge_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
        assert merge_counts == initial_counts

        # check changes where merged in
        with pipeline.sql_client() as sql_client:
            rows = sql_client.execute_sql("SELECT number FROM issues WHERE id = 1232152492 LIMIT 1")
            assert rows[0][0] == 105
            rows = sql_client.execute_sql("SELECT number FROM issues WHERE id = 1142699354 LIMIT 1")
            assert rows[0][0] == 300

    # test append
    info = pipeline.run(github().load_issues, write_disposition="append", loader_file_format=destination_config.file_format)
    assert_load_info(info)
    assert pipeline.default_schema.tables["issues"]["write_disposition"] == "append"
    # the counts of all tables must be double
    append_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert {k:v*2 for k, v in initial_counts.items()} == append_counts

    # test replace
    info = pipeline.run(github().load_issues, write_disposition="replace", loader_file_format=destination_config.file_format)
    assert_load_info(info)
    assert pipeline.default_schema.tables["issues"]["write_disposition"] == "replace"
    # the counts of all tables must be double
    replace_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert replace_counts == initial_counts


@pytest.mark.parametrize("destination_config", destinations_configs(all_staging_configs=True), ids=lambda x: x.name)
def test_all_data_types(destination_config: DestinationTestConfiguration) -> None:

    pipeline = destination_config.setup_pipeline('test_stage_loading', dataset_name="test_all_data_types" + uniq_id())

    # Redshift parquet -> exclude col7_precision
    # redshift and athena, parquet and jsonl, exclude time types
    exclude_types: List[TDataType] = []
    exclude_columns: List[str] = []
    if destination_config.destination in ("redshift", "athena") and destination_config.file_format in ('parquet', 'jsonl'):
        # Redshift copy doesn't support TIME column
        exclude_types.append("time")
    if destination_config.destination == "redshift" and destination_config.file_format in ("parquet", "jsonl"):
        # Redshift can't load fixed width binary columns from parquet
        exclude_columns.append("col7_precision")

    column_schemas, data_types = table_update_and_row(exclude_types=exclude_types, exclude_columns=exclude_columns)

    # bigquery cannot load into JSON fields from parquet
    if destination_config.file_format == "parquet":
        if destination_config.destination == "bigquery":
            # change datatype to text and then allow for it in the assert (parse_complex_strings)
            column_schemas["col9_null"]["data_type"] = column_schemas["col9"]["data_type"] = "text"
    # redshift cannot load from json into VARBYTE
    if destination_config.file_format == "jsonl":
        if destination_config.destination == "redshift":
            # change the datatype to text which will result in inserting base64 (allow_base64_binary)
            binary_cols = ["col7", "col7_null"]
            for col in binary_cols:
                column_schemas[col]["data_type"] = "text"

    # apply the exact columns definitions so we process complex and wei types correctly!
    @dlt.resource(table_name="data_types", write_disposition="merge", columns=column_schemas)
    def my_resource():
        nonlocal data_types
        yield [data_types]*10

    @dlt.source(max_table_nesting=0)
    def my_source():
        return my_resource

    info = pipeline.run(my_source(), loader_file_format=destination_config.file_format)
    assert_load_info(info)

    with pipeline.sql_client() as sql_client:
        db_rows = sql_client.execute_sql("SELECT * FROM data_types")
        assert len(db_rows) == 10
        db_row = list(db_rows[0])
        # parquet is not really good at inserting json, best we get are strings in JSON columns
        parse_complex_strings = destination_config.file_format == "parquet" and destination_config.destination in ["redshift", "bigquery", "snowflake"]
        allow_base64_binary = destination_config.file_format == "jsonl" and destination_config.destination in ["redshift"]
        # content must equal
        assert_all_data_types_row(
            db_row[:-2],
            parse_complex_strings=parse_complex_strings,
            allow_base64_binary=allow_base64_binary,
            timestamp_precision=sql_client.capabilities.timestamp_precision,
            schema=column_schemas
        )
