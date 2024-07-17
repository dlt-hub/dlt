from typing import Generator, Dict

import pytest

import dlt
from dlt.destinations import filesystem
from tests.load.utils import GCS_BUCKET
from tests.pipeline.utils import assert_load_info


@pytest.mark.essential
def test_clickhouse_gcs_s3_compatibility() -> None:
    @dlt.resource
    def dummy_data() -> Generator[Dict[str, int], None, None]:
        yield {"field1": 1, "field2": 2}

    gcp_bucket = filesystem(
        GCS_BUCKET.replace("gs://", "s3://"), destination_name="filesystem_s3_gcs_comp"
    )

    pipe = dlt.pipeline(
        pipeline_name="gcs_s3_compatibility",
        destination="clickhouse",
        staging=gcp_bucket,
        dev_mode=True,
    )
    pack = pipe.run([dummy_data])
    assert_load_info(pack)
