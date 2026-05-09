from os import environ
from typing import Any, Iterator

import pytest

from dlt._workspace.cli import echo as fmt


@pytest.fixture()
def environment() -> Iterator[Any]:
    """Clear environ for the test, restore via autouse preserve_environ."""
    environ.clear()
    yield environ


@pytest.fixture(autouse=True)
def _ai_host_dlthub() -> Iterator[None]:
    """Sets the active CLI host to `dlthub` for AI tests (ai is dlthub-only)."""
    # in-process tests need the host set so cli_cmd() matches dlthub-style output
    previous = fmt.get_cli_host_name()
    fmt.set_cli_host_name("dlthub")
    try:
        yield
    finally:
        fmt.set_cli_host_name(previous)
