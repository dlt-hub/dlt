import pytest
import dlt

from tests.utils import get_test_worker_id


@pytest.fixture
def lancedb_destination(tmp_path_factory):
    path = tmp_path_factory.mktemp(f"lancedb_{get_test_worker_id()}")
    return dlt.destinations.lancedb(lance_uri=str(path))
