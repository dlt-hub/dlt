import os

from tests.utils import (
    patch_home_dir,
    autouse_test_storage,
    preserve_environ,
    duckdb_pipeline_location,
    wipe_pipeline,
    setup_secret_providers_to_current_module,
)


def pytest_configure(config):
    # push sentry to ci
    os.environ["RUNTIME__SENTRY_DSN"] = (
        "https://6f6f7b6f8e0f458a89be4187603b55fe@o1061158.ingest.sentry.io/4504819859914752"
    )
