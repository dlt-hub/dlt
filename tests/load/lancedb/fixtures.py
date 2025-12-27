import pytest
import dlt


@pytest.fixture
def lancedb_destination(tmp_path_factory, worker_id):
    path = tmp_path_factory.mktemp(f"lancedb-{worker_id}")
    return dlt.destinations.lancedb(lance_uri=str(path))
