import sys
from typing import Iterator
import pytest

from dlt._workspace._workspace_context import WorkspaceRunContext

from tests.workspace.utils import isolated_workspace


@pytest.fixture(autouse=True)
def auto_isolated_workspace(
    autouse_test_storage, preserve_run_context
) -> Iterator[WorkspaceRunContext]:
    """Creates new isolated `empty` workspace in `_storage` (top level) folder. Makes sure that _storage
    folder is cleaned first and that previous run context (be it workspace, oss or project) is restored
    after test executes.
    """
    # activate `dev` profile which is a default. many tests use run pipelines in remote processes
    # and this profile will be assumed automatically
    with isolated_workspace("empty", profile="dev") as ctx:
        yield ctx
