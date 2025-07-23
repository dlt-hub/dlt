# useful fixtures for dlt tests, preserve_environ etc.
# NOTE: every fixture should have a a really good docstring about what it does

import pytest
from os import environ


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