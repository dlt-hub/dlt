import pytest
import pytz
import datetime # noqa: I251
import dateutil.parser

from pathlib import Path
import dlt, os
from dlt.common import json, Decimal
from copy import deepcopy

from tests.load.pipeline.test_merge_disposition import github
from tests.load.pipeline.utils import  load_table_counts
from tests.pipeline.utils import  assert_load_info
from tests.load.utils import TABLE_ROW_ALL_DATA_TYPES

# dlt_gcs

staging_combinations_fields = "destination,file_format,bucket,storage_integration"
staging_combinations = [
    ("redshift","parquet","tests.bucket_url_aws", ""),
    ("redshift","jsonl","tests.bucket_url_aws", ""),
    ("bigquery","parquet","tests.bucket_url_gcs", ""),
    ("bigquery","jsonl","tests.bucket_url_gcs", ""),
    ("snowflake","jsonl","tests.bucket_url_awst", ""), # "dlt_s3"),
    ("snowflake","jsonl","tests.bucket_url_gcs", "dlt_gcs")
    ]

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


@pytest.mark.parametrize(staging_combinations_fields, staging_combinations)
def test_staging_load(destination: str, file_format: str, bucket: str, storage_integration: str) -> None:

    bucket = dlt.config.get(bucket, str)

    # snowflake requires gcs prefix instead of gs in bucket path
    if destination == "snowflake":
        bucket = bucket.replace("gs://", "gcs://")

    # set env vars
    os.environ['DESTINATION__FILESYSTEM__BUCKET_URL'] = bucket
    os.environ['DESTINATION__STORAGE_INTEGRATION'] = storage_integration

    pipeline = dlt.pipeline(pipeline_name='test_stage_loading_5', destination=destination, staging="filesystem", dataset_name='staging_test', full_refresh=True)

    info = pipeline.run(github(), loader_file_format=file_format)
    assert_load_info(info)
    package_info = pipeline.get_load_package_info(info.loads_ids[0])
    assert package_info.state == "loaded"

    assert len(package_info.jobs["failed_jobs"]) == 0
    # we have 4 parquet and 4 reference jobs plus one merge job
    assert len(package_info.jobs["completed_jobs"]) == 9
    assert len([x for x in package_info.jobs["completed_jobs"] if x.job_file_info.file_format == "reference"]) == 4
    assert len([x for x in package_info.jobs["completed_jobs"] if x.job_file_info.file_format == file_format]) == 4
    assert len([x for x in package_info.jobs["completed_jobs"] if x.job_file_info.file_format == "sql"]) == 1

    initial_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert initial_counts["issues"] == 100

    # check item of first row in db
    with pipeline._get_destination_client(pipeline.default_schema) as client:
        rows = client.sql_client.execute_sql("SELECT url FROM issues WHERE id = 388089021 LIMIT 1")
        assert rows[0][0] == "https://api.github.com/repos/duckdb/duckdb/issues/71"

    # test merging in some changed values
    info = pipeline.run(load_modified_issues)
    assert_load_info(info)
    assert pipeline.default_schema.tables["issues"]["write_disposition"] == "merge"
    merge_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert merge_counts == initial_counts

    # check changes where merged in
    with pipeline._get_destination_client(pipeline.default_schema) as client:
        rows = client.sql_client.execute_sql("SELECT number FROM issues WHERE id = 1232152492 LIMIT 1")
        assert rows[0][0] == 105
        rows = client.sql_client.execute_sql("SELECT number FROM issues WHERE id = 1142699354 LIMIT 1")
        assert rows[0][0] == 300

    # test append
    info = pipeline.run(github().load_issues, write_disposition="append")
    assert_load_info(info)
    assert pipeline.default_schema.tables["issues"]["write_disposition"] == "append"
    # the counts of all tables must be double
    append_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert {k:v*2 for k, v in initial_counts.items()} == append_counts

    # test replace
    info = pipeline.run(github().load_issues, write_disposition="replace")
    assert_load_info(info)
    assert pipeline.default_schema.tables["issues"]["write_disposition"] == "replace"
    # the counts of all tables must be double
    replace_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert replace_counts == initial_counts


# @pytest.mark.skip(reason="need to discuss")
@pytest.mark.parametrize(staging_combinations_fields, staging_combinations)
def test_all_data_types(destination: str, file_format: str, bucket: str, storage_integration: str) -> None:
    # set env vars
    os.environ['DESTINATION__FILESYSTEM__BUCKET_URL'] = bucket
    os.environ['DESTINATION__STORAGE_INTEGRATION'] = storage_integration
    pipeline = dlt.pipeline(pipeline_name='test_stage_loading', destination=destination, dataset_name='staging_test', full_refresh=True)

    global data_types
    data_types = deepcopy(TABLE_ROW_ALL_DATA_TYPES)

    @dlt.resource(table_name="data_types", write_disposition="merge")
    def my_resource():
        global data_types
        yield data_types

    info = pipeline.run(my_resource())
    assert_load_info(info)
    with pipeline._get_destination_client(pipeline.default_schema) as client:
        sent_values = list(data_types.values())
        # create datetime object and add utc timezone info
        sent_values[3] = dateutil.parser.isoparse(sent_values[3]).replace(tzinfo=datetime.timezone.utc)

        # change precision of decimal...
        sent_values[5] = Decimal(str(sent_values[5]) + ("0" * 7))

        # bytes get saved as hex on redshift
        if destination == "redshift":
            sent_values[6] = sent_values[6].hex()

        # for the complex value only the second value is stored, I don't think this is right..
        sent_values[8] = sent_values[8]["link"]

        rows = client.sql_client.execute_sql("SELECT * FROM data_types")
        assert sent_values == [val for val in rows[0]][0:-2]
