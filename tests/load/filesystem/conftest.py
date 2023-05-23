import os
from typing import Iterator
from pathlib import Path

import pytest

import dlt
from dlt.common.schema import Schema
from dlt.common.utils import uniq_id
from dlt.destinations.filesystem.filesystem import FilesystemClient

from tests.utils import preserve_environ
from tests.load.filesystem.utils import get_client


# env variables for URLs for all test buckets, e.g. "gcs://bucket_name", "s3://bucket_name", "file://bucket_name"
bucket_env_vars = [
    "tests.bucket_url_gcs", "tests.bucket_url_aws", "tests.bucket_url_file"
]

ALL_BUCKETS = [b for b in (dlt.config.get(var, str) for var in bucket_env_vars) if b]


@pytest.fixture(scope='function', params=ALL_BUCKETS)
def all_buckets_env(request) -> Iterator[str]:  # type: ignore[no-untyped-def]
    """Parametrized fixture to configure filesystem destination bucket in env for each test bucket
    """
    os.environ['DESTINATION__FILESYSTEM__BUCKET_URL'] = request.param
    yield request.param


@pytest.fixture(scope='function')
def filesystem_client() -> Iterator[FilesystemClient]:
    schema = Schema('test_schema')
    dataset_name = 'test_' + uniq_id()
    _client = get_client(schema, dataset_name)
    dataset_path = str(Path(_client.fs_path).joinpath(dataset_name))
    try:
        yield _client
    finally:
        _client.fs_client.rm(dataset_path, recursive=True)
