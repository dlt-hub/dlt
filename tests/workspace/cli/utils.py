import os
import pytest
import shutil
from typing import Iterator

from dlt.common.libs import git
from dlt.common.pipeline import get_dlt_repos_dir
from dlt.common.storages.file_storage import FileStorage
from dlt.common.utils import uniq_id

from dlt.sources import SourceReference

from dlt._workspace.cli import echo, DEFAULT_VERIFIED_SOURCES_REPO, DEFAULT_VIBE_SOURCES_REPO

from tests.utils import TEST_STORAGE_ROOT
from tests.workspace.utils import EMPTY_WORKSPACE_DIR


INIT_REPO_LOCATION = DEFAULT_VERIFIED_SOURCES_REPO
INIT_REPO_BRANCH = "master"
INIT_VIBE_REPO_LOCATION = DEFAULT_VIBE_SOURCES_REPO
INIT_VIBE_REPO_BRANCH = "main"
WORKSPACE_CLI_CASES_DIR = os.path.abspath(os.path.join("tests", "workspace", "cli", "cases"))
REPO_ROOT = os.path.abspath(TEST_STORAGE_ROOT)


@pytest.fixture(autouse=True)
def auto_echo_default_choice() -> Iterator[None]:
    """Always answer default in CLI interactions"""
    echo.ALWAYS_CHOOSE_DEFAULT = True
    yield
    echo.ALWAYS_CHOOSE_DEFAULT = False


@pytest.fixture(scope="module")
def cloned_init_repo() -> FileStorage:
    return git.get_fresh_repo_files(
        INIT_REPO_LOCATION, get_dlt_repos_dir(), branch=INIT_REPO_BRANCH
    )


@pytest.fixture(scope="module")
def cloned_init_vibe_repo() -> FileStorage:
    return git.get_fresh_repo_files(
        DEFAULT_VIBE_SOURCES_REPO, get_dlt_repos_dir(), branch=INIT_VIBE_REPO_BRANCH
    )


@pytest.fixture
def repo_dir(cloned_init_repo: FileStorage) -> str:
    return get_repo_dir(cloned_init_repo, f"verified_sources_repo_{uniq_id()}")


@pytest.fixture
def vibe_repo_dir(cloned_init_vibe_repo: FileStorage) -> str:
    return get_repo_dir(cloned_init_vibe_repo, f"vibe_sources_repo_{uniq_id()}")


@pytest.fixture
def workspace_files() -> Iterator[FileStorage]:
    workspace_files = get_workspace_files()
    yield workspace_files


def get_repo_dir(cloned_repo: FileStorage, repo_name: str) -> str:
    repo_dir = os.path.join(REPO_ROOT, repo_name)
    shutil.copytree(cloned_repo.storage_path, repo_dir)
    return repo_dir


def get_workspace_files(clear_all_sources: bool = True) -> FileStorage:
    # we only remove sources registered outside of dlt core
    for name, source in SourceReference.SOURCES.copy().items():
        if not source.ref.startswith("dlt.sources") and not source.ref.startswith(
            "default_pipeline"
        ):
            SourceReference.SOURCES.pop(name)

    if clear_all_sources:
        SourceReference.SOURCES.clear()

    # project dir
    return FileStorage(EMPTY_WORKSPACE_DIR, makedirs=False)
