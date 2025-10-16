from contextlib import contextmanager
import os
import shutil
from typing import Generator, Iterator

import pytest

from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.pluggable_run_context import RunContextBase, PluggableRunContext
from dlt.common.runtime.run_context import switch_context
from dlt.common.storages.file_storage import FileStorage
from dlt.common.utils import set_working_dir

from dlt._workspace._workspace_context import WorkspaceRunContext

from tests.utils import TEST_STORAGE_ROOT

WORKSPACE_CASES_DIR = os.path.abspath(os.path.join("tests", "workspace", "cases", "workspaces"))
TEST_STORAGE_ROOT = os.path.abspath(TEST_STORAGE_ROOT)
EMPTY_WORKSPACE_DIR = os.path.join(TEST_STORAGE_ROOT, "empty")


@contextmanager
def isolated_workspace(
    name: str, profile: str = None, required: str = "WorkspaceRunContext"
) -> Iterator[WorkspaceRunContext]:
    """Copies `name` workspace from WORKSPACE_CASES_DIR to `_storage` top level folder
    changes cwd to a workspace copy and activates it to create a fully isolated workspace.
    Note that global_dit is patched (TODO: replace with workspace config)
    """
    new_run_dir = restore_clean_workspace(name)
    with set_working_dir(new_run_dir):
        ctx = switch_context(new_run_dir, profile=profile, required=required)
        assert ctx.run_dir == new_run_dir
        # also mock global dir so it does not point to default user ~
        if isinstance(ctx, WorkspaceRunContext):
            ctx._global_dir = os.path.abspath(".global_dir")
            # reload toml provides after patching
            Container()[PluggableRunContext].reload_providers()
        yield ctx  # type: ignore


def restore_clean_workspace(name: str) -> str:
    """Create a fresh copy of a workspace for a test run.

    If the current working directory is inside the target workspace directory, temporarily
    change the working directory outside of it before deletion to avoid Windows errors.

    Args:
        name: Name of the workspace case to restore.

    Returns:
        Absolute path to the restored workspace directory.
    """
    source_workspace_dir = os.path.join(WORKSPACE_CASES_DIR, name)
    new_run_dir = os.path.join(TEST_STORAGE_ROOT, name)

    # ensure parent exists before copying
    os.makedirs(TEST_STORAGE_ROOT, exist_ok=True)

    # if cwd is within the target directory, move out temporarily to allow deletion
    cwd = os.path.abspath(os.getcwd())
    target = os.path.abspath(new_run_dir)
    try:
        is_within_target = os.path.commonpath([cwd, target]) == target
    except ValueError:
        # paths on different drives on windows
        is_within_target = False

    # use a single code path, switching cwd only when needed
    from contextlib import nullcontext

    cm = set_working_dir(TEST_STORAGE_ROOT) if is_within_target else nullcontext()
    with cm:
        if os.path.isdir(new_run_dir):
            shutil.rmtree(new_run_dir, onerror=FileStorage.rmtree_del_ro)
        shutil.copytree(source_workspace_dir, new_run_dir, dirs_exist_ok=True)

    return new_run_dir


@pytest.fixture
def pokemon_pipeline_context() -> Generator[RunContextBase, None, None]:
    from dlt._workspace._templates._core_source_templates.rest_api_pipeline import load_pokemon

    with isolated_workspace(name="pipelines") as ctx:
        load_pokemon()
        yield ctx


@pytest.fixture
def fruitshop_pipeline_context() -> Generator[RunContextBase, None, None]:
    from dlt._workspace._templates._single_file_templates.fruitshop_pipeline import load_shop

    with isolated_workspace(name="pipelines") as ctx:
        load_shop()
        yield ctx
