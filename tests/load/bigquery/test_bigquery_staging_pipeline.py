import pytest
from pathlib import Path

import dlt, os
from dlt.common.utils import uniq_id


@pytest.mark.parametrize("file_format", ["parquet", "jsonl"])
def test_bigquery_staging_load(file_format: str) -> None:

    # set gcs bucket url
    os.environ['DESTINATION__FILESYSTEM__BUCKET_URL'] = "gs://ci-test-bucket"
    pipeline = dlt.pipeline(pipeline_name='bq_staging_parquet_test_' + uniq_id(), destination="bigquery", staging="filesystem", dataset_name='bigquery_staging_test_' + uniq_id())

    @dlt.resource(primary_key='id')
    def some_data():  # type: ignore[no-untyped-def]
        yield [{'id': 1}, {'id': 2}, {'id': 3}]

    @dlt.resource
    def other_data():  # type: ignore[no-untyped-def]
        yield [1, 2, 3, 4, 5]

    @dlt.source
    def some_source():  # type: ignore[no-untyped-def]
        return [some_data(), other_data()]

    info = pipeline.run(some_source(), loader_file_format=file_format)
    package_info = pipeline.get_load_package_info(info.loads_ids[0])
    assert package_info.state == "loaded"

    # all three jobs succeeded
    assert len(package_info.jobs["failed_jobs"]) == 0
    # we have 3 parquet and 3 reference jobs
    assert len(package_info.jobs["completed_jobs"]) == 6
    assert len([x for x in package_info.jobs["completed_jobs"] if x.job_file_info.file_format == "reference"]) == 3
    assert len([x for x in package_info.jobs["completed_jobs"] if x.job_file_info.file_format == file_format]) == 3

    # check data in redshift
    with pipeline._get_destination_client(pipeline.default_schema) as client:
        rows = client.sql_client.execute_sql("SELECT * FROM some_data")
        assert len(rows) == 3
        rows = client.sql_client.execute_sql("SELECT * FROM other_data")
        assert len(rows) == 5


