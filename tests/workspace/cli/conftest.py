# MUST be before importing dlt
import os
from pathlib import Path
import tempfile
from tests.utils import get_test_worker_id

worker_id = get_test_worker_id()

env_root = Path(tempfile.gettempdir()).resolve() / "dlt-env" / worker_id
home_dir = env_root / "home"
xdg_dir = env_root / "xdg"

home_dir.mkdir(parents=True, exist_ok=True)
(xdg_dir / "dlt" / "repos").mkdir(parents=True, exist_ok=True)

os.environ["HOME"] = str(home_dir)
os.environ["XDG_DATA_HOME"] = str(xdg_dir)

###############

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
