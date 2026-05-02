import sys
from typing import Any, Iterator
import pytest

from dlt._workspace._workspace_context import WorkspaceRunContext
from dlt._workspace.cli import _run_command as run_cmd_mod
from dlt._workspace.cli import utils as cli_utils_mod

from tests.workspace.utils import isolated_workspace

# Import fixtures from utils to make them available to all tests
from tests.workspace.cli.utils import (  # noqa: F401
    _cached_init_repo,
    cloned_init_repo,
    repo_dir,
    auto_echo_default_choice,
)


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


@pytest.fixture(autouse=True)
def isolated_manifest_loading(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force manifest_from_module (via _run_command.load_manifest) to run in a
    subprocess so tests don't accumulate sys.modules / sys.path / FileFinder
    cache entries across the workspace rmtree+recreate cycle between tests.
    """
    original = run_cmd_mod.manifest_from_module

    def _isolated(name_or_path: str, use_all: bool = True) -> Any:
        return original(name_or_path, use_all=use_all, isolated=True)

    monkeypatch.setattr(run_cmd_mod, "manifest_from_module", _isolated)
    monkeypatch.setattr(cli_utils_mod, "manifest_from_module", _isolated)
