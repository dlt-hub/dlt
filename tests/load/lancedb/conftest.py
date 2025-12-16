import os
import pytest
import dlt


@pytest.fixture
def lancedb_destination():
    worker = os.environ.get("PYTEST_XDIST_WORKER", "serial")
    return dlt.destinations.lancedb(lance_uri=f"/tmp/.lancedb/{worker}")
