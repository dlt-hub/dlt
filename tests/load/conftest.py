import os
import pytest
from typing import Iterator

# TODO Synapse: address broken 'DEFAULT_BUCKETS"
#from tests.load.utils import DEFAULT_BUCKETS, ALL_BUCKETS
from tests.load.utils import ALL_BUCKETS
from tests.utils import preserve_environ


@pytest.fixture(scope='function', params=DEFAULT_BUCKETS)
def default_buckets_env(request) -> Iterator[str]:
    """Parametrized fixture to configure filesystem destination bucket in env for each test bucket
    """
    os.environ['DESTINATION__FILESYSTEM__BUCKET_URL'] = request.param
    yield request.param



@pytest.fixture(scope='function', params=ALL_BUCKETS)
def all_buckets_env(request) -> Iterator[str]:
    if isinstance(request.param, dict):
        bucket_url = request.param['bucket_url']
        # R2 bucket needs to override all credentials
        for key, value in request.param['credentials'].items():
            os.environ[f'DESTINATION__FILESYSTEM__CREDENTIALS__{key.upper()}'] = value
    else:
        bucket_url = request.param
    os.environ['DESTINATION__FILESYSTEM__BUCKET_URL'] = bucket_url
    yield bucket_url
