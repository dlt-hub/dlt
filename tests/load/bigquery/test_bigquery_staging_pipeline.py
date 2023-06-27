import pytest
from pathlib import Path

import dlt, os
from dlt.common.utils import uniq_id


def test_bigquery_parquet_staging_load() -> None:

    # set aws bucket url
    os.environ['DESTINATION__FILESYSTEM__BUCKET_URL'] = "gs://ci-test-bucket"
    pipeline = dlt.pipeline(pipeline_name='bq_staging_parquet_test_' + uniq_id(), destination="bigquery", staging="filesystem", dataset_name='bigquery_copy_parquet_test_' + uniq_id())

    @dlt.resource(primary_key='id')
    def some_data():  # type: ignore[no-untyped-def]
        yield [{'id': 1}, {'id': 2}, {'id': 3}]

    @dlt.resource
    def other_data():  # type: ignore[no-untyped-def]
        yield [1, 2, 3, 4, 5]

    @dlt.source
    def some_source():  # type: ignore[no-untyped-def]
        return [some_data(), other_data()]

    info = pipeline.run(some_source())
    package_info = pipeline.get_load_package_info(info.loads_ids[0])
    assert package_info.state == "loaded"

    # all three jobs succeeded
    assert len(package_info.jobs["failed_jobs"]) == 0
    # we have 3 parquet and 3 reference jobs
    assert len(package_info.jobs["completed_jobs"]) == 6


# @pytest.mark.skip(reason="giving some unclear error")
def test_bigquery_jsonl_staging_load() -> None:

    # set aws bucket url
    os.environ['DESTINATION__FILESYSTEM__BUCKET_URL'] = "gs://ci-test-bucket"
    pipeline = dlt.pipeline(pipeline_name='bq_staging_jsonl_test_' + uniq_id(), destination="bigquery", staging="filesystem", dataset_name='bigquery_copy_parquet_test_' + uniq_id())

    @dlt.resource(primary_key='id')
    def some_data():  # type: ignore[no-untyped-def]
        yield [{'id': 1}, {'id': 2}, {'id': 3}]

    @dlt.resource
    def other_data():  # type: ignore[no-untyped-def]
        yield [1, 2, 3, 4, 5]

    @dlt.source
    def some_source():  # type: ignore[no-untyped-def]
        return [some_data(), other_data()]

    info = pipeline.run(some_source(), loader_file_format="jsonl")
    package_info = pipeline.get_load_package_info(info.loads_ids[0])
    assert package_info.state == "loaded"

    # all three jobs succeeded
    assert len(package_info.jobs["failed_jobs"]) == 0
    # we have 3 parquet and 3 reference jobs
    assert len(package_info.jobs["completed_jobs"]) == 6
