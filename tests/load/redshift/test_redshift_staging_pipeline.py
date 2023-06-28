import pytest
from pathlib import Path

import dlt, os
from dlt.common.utils import uniq_id
from dlt.pipeline.exceptions import PipelineStepFailed

def test_redshift_parquet_staging_load() -> None:

    # set aws bucket url
    os.environ['DESTINATION__FILESYSTEM__BUCKET_URL'] = "s3://dlt-ci-test-bucket"
    pipeline = dlt.pipeline(pipeline_name='parquet_test_' + uniq_id(), destination="redshift", staging="filesystem", dataset_name='redshift_copy_parquet_test_' + uniq_id())

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




    os.environ['DESTINATION__FORWARD_STAGING_CREDENTIALS'] = "False"
    # test that credentials are not forwarded if setting disabled
    with pytest.raises(PipelineStepFailed):
        pipeline.run(some_source())
