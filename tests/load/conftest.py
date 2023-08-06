import os
import pytest
from typing import Iterator

from tests.load.utils import ALL_BUCKETS
from tests.utils import preserve_environ


@pytest.fixture(scope='function', params=ALL_BUCKETS)
def all_buckets_env(request) -> Iterator[str]:  # type: ignore[no-untyped-def]
    """Parametrized fixture to configure filesystem destination bucket in env for each test bucket
    """
    os.environ['DESTINATION__FILESYSTEM__BUCKET_URL'] = request.param
    yield request.param

