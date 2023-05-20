import os
from typing import Iterator

import pytest

import dlt

from tests.utils import preserve_environ

# URLs for all test buckets, e.g. "gcs://bucket_name", "s3://bucket_name", "file://bucket_name"
bucket_env_vars = [
    "TEST_BUCKET_URL_GCS", "TEST_BUCKET_URL_AWS", "TEST_BUCKET_URL_FILE"
]

ALL_BUCKETS = [b for b in (dlt.config.get(var, str) for var in bucket_env_vars) if b]


@pytest.fixture(scope='function', params=ALL_BUCKETS)
def all_buckets_env(request) -> Iterator[str]:  # type: ignore[no-untyped-def]
    """Parametrized fixture to configure filesystem destination bucket in env for each test bucket
    """
    os.environ['DESTINATION__FILESYSTEM__BUCKET_URL'] = request.param
    yield request.param
