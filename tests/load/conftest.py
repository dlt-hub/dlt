from typing import Iterator, Tuple
import os

import pytest

import dlt

from tests.utils import preserve_environ
from tests.utils import ALL_DESTINATIONS
from tests.load.utils import ALL_BUCKETS


@pytest.fixture(scope='function', params=ALL_BUCKETS)
def all_buckets_env(request) -> Iterator[str]:  # type: ignore[no-untyped-def]
    """Parametrized fixture to configure filesystem destination bucket in env for each test bucket
    """
    os.environ['DESTINATION__FILESYSTEM__BUCKET_URL'] = request.param
    yield request.param


_destinations_and_buckets = [(d, ) for d in ALL_DESTINATIONS] + [('filesystem', b) for b in ALL_BUCKETS]
_param_ids = [':'.join(p) for p in _destinations_and_buckets]

@pytest.fixture(scope='function', params=_destinations_and_buckets, ids=_param_ids)
def any_destination(request) -> Iterator[Tuple[str, ...]]:  # type: ignore[no-untyped-def]
    """Parametrized fixture iterating all destinations including filesystem configured for each test bucket"""
    destination = request.param[0]
    if len(request.param) > 1:
        os.environ['DESTINATION__FILESYSTEM__BUCKET_URL'] = request.param[1]
    yield destination
