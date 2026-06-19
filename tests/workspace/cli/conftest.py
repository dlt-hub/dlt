import os
import sys
from typing import Any, Iterator
import pytest

from dlt.common import known_env
from dlt.common.runtime.run_context import RunContext

from dlt._workspace._workspace_context import WorkspaceRunContext
from dlt._workspace.cli import echo as fmt
from dlt._workspace.cli.dlthub import utils as dlthub_utils_mod
from dlt._workspace.deployment import _run_helpers as run_helpers_mod

from tests.utils import get_test_storage_root, unload_modules_at_path
from tests.workspace.utils import isolated_workspace


@pytest.fixture(autouse=True)
def reset_echo_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """Resets echo interactivity globals before each test in tests/workspace/cli."""
    monkeypatch.setattr(fmt, "ALWAYS_CHOOSE_DEFAULT", False)
    monkeypatch.setattr(fmt, "ALWAYS_CHOOSE_VALUE", None)
    monkeypatch.setattr(fmt, "ALWAYS_CONFIRM", False)


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


@pytest.fixture
def legacy_workspace_context(monkeypatch: pytest.MonkeyPatch) -> Iterator[RunContext]:
    """Nests a legacy (non-workspace) `RunContext` inside `auto_isolated_workspace`."""
    # with an active workspace the `dlt` host falls back to `dlthub`'s slim command set; the
    # legacy context restores the full legacy `dlt init`/`deploy`/`pipeline` command set
    with isolated_workspace("legacy", required="RunContext") as ctx:
        # pass working dir to subprocesses
        monkeypatch.setenv(known_env.DLT_DATA_DIR, os.path.join(ctx.run_dir, ".dlt"))
        yield ctx  # type: ignore[misc]


@pytest.fixture(autouse=True)
def isolated_manifest_loading(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force `manifest_from_module` to run in a subprocess so tests don't accumulate
    sys.modules / sys.path / FileFinder cache entries across workspace rmtree+recreate.
    """
    original = run_helpers_mod.manifest_from_module

    def _isolated(name_or_path: str, use_all: bool = True) -> Any:
        return original(name_or_path, use_all=use_all, isolated=True)

    monkeypatch.setattr(run_helpers_mod, "manifest_from_module", _isolated)
    monkeypatch.setattr(dlthub_utils_mod, "manifest_from_module", _isolated)


@pytest.fixture(autouse=True)
def auto_unload_init_modules() -> Iterator[None]:
    """Unloads modules that were imported wfrom temp storage"""
    try:
        yield
    finally:
        unload_modules_at_path(get_test_storage_root())
