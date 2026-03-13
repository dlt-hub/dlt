from os import environ
from typing import Any, Iterator

import pytest


@pytest.fixture()
def environment() -> Iterator[Any]:
    """Clear environ for the test, restore via autouse preserve_environ."""
    environ.clear()
    yield environ
