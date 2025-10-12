from contextlib import contextmanager
import os
import shutil
from typing import Generator, Iterator

import pytest

from dlt.common.configuration.specs.pluggable_run_context import RunContextBase
from dlt.common.runtime.run_context import switch_context
from dlt.common.utils import set_working_dir

from dlt._workspace._templates._core_source_templates.rest_api_pipeline import load_pokemon
from dlt._workspace._templates._single_file_templates.fruitshop_pipeline import load_shop
from dlt._workspace._workspace_context import WorkspaceRunContext

from tests.utils import TEST_STORAGE_ROOT

WORKSPACE_CASES_DIR = os.path.join("tests", "workspace/cases/workspaces")


@contextmanager
def isolated_workspace(
    source_project_dir: str, name: str, profile: str = None, required: str = "WorkspaceRunContext"
) -> Iterator[WorkspaceRunContext]:
    new_run_dir = os.path.abspath(os.path.join(TEST_STORAGE_ROOT, name))
    shutil.copytree(source_project_dir, new_run_dir, dirs_exist_ok=True)

    with set_working_dir(new_run_dir):
        ctx = switch_context(new_run_dir, profile=profile, required=required)
        assert ctx.run_dir == new_run_dir
        yield ctx  # type: ignore


@pytest.fixture
def pokemon_pipeline_context() -> Generator[RunContextBase, None, None]:
    with isolated_workspace(
        os.path.join(WORKSPACE_CASES_DIR, "pipelines"), name="pipelines"
    ) as ctx:
        load_pokemon()
        yield ctx


@pytest.fixture
def fruitshop_pipeline_context() -> Generator[RunContextBase, None, None]:
    with isolated_workspace(
        os.path.join(WORKSPACE_CASES_DIR, "pipelines"), name="pipelines"
    ) as ctx:
        load_shop()
        yield ctx
