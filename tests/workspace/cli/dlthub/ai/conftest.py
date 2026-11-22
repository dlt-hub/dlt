from os import environ
from typing import Any, Iterator

import pytest

from dlt._workspace.cli import echo as fmt


@pytest.fixture(autouse=True)
def _snowflake_home(
    tmp_path_factory: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch
) -> Any:
    """Points `SNOWFLAKE_HOME` to a temp dir: cortex installs write the user MCP config there."""
    snowflake_home = tmp_path_factory.mktemp("snowflake_home")
    monkeypatch.setenv("SNOWFLAKE_HOME", str(snowflake_home))
    return snowflake_home


@pytest.fixture()
def environment(_snowflake_home: Any) -> Iterator[Any]:
    """Clear environ for the test, restore via autouse preserve_environ. `SNOWFLAKE_HOME` stays
    so a cortex install never writes to the real user config."""
    environ.clear()
    environ["SNOWFLAKE_HOME"] = str(_snowflake_home)
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
