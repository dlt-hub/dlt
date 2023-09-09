import os
import pytest

from tests.utils import patch_home_dir, autouse_test_storage, preserve_environ, duckdb_pipeline_location, wipe_pipeline

@pytest.fixture(autouse=True)
def set_working_dir():
    # always set working dir to main website folder
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)

def pytest_configure(config):
    # push sentry to ci
    os.environ["RUNTIME__SENTRY_DSN"] = "https://6f6f7b6f8e0f458a89be4187603b55fe@o1061158.ingest.sentry.io/4504819859914752"
