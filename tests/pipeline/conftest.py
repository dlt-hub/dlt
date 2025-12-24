import pytest
from os import environ


from tests.utils import (
    preserve_environ,
    autouse_test_storage,
    auto_test_run_context,
    deactivate_pipeline,
    test_storage,
)
from tests.common.configuration.utils import environment, toml_providers


@pytest.fixture(autouse=True)
def drop_dataset_from_env() -> None:
    """Remove the ``DATASET_NAME`` environment variable before each test.

    This autouse fixture guarantees that tests start with a clean environment. Some
    pipelines derive the default destination dataset name from the environment
    variable ``DATASET_NAME`` – if it is left over from a previous test run the
    execution could pick up unexpected state. Clearing it here prevents such
    flakiness.
    """
    if "DATASET_NAME" in environ:
        del environ["DATASET_NAME"]