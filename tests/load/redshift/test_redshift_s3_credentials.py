import os
import pytest
import dlt

from dlt.pipeline.exceptions import PipelineStepFailed

from tests.load.pipeline.test_merge_disposition import github


def test_forward_credentials_settings() -> None:

    # set bucket url
    os.environ['DESTINATION__FILESYSTEM__BUCKET_URL'] = "s3://dlt-ci-test-bucket"
    pipeline = dlt.pipeline(pipeline_name='test_stage_loading', destination="redshift", staging="filesystem", dataset_name='staging_test', full_refresh=True)

    os.environ['DESTINATION__FORWARD_STAGING_CREDENTIALS'] = "False"
    with pytest.raises(PipelineStepFailed):
        pipeline.run({github()})