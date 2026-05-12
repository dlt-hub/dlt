import os
import sys
import pytest
import shutil
from typing import Iterator
from dlt.common.libs import git
from dlt.common.pipeline import get_dlt_repos_dir
from dlt.common.storages.file_storage import FileStorage
from dlt.common.utils import uniq_id

from dlt.sources import SourceReference

from dlt._workspace.cli import echo, DEFAULT_VERIFIED_SOURCES_REPO

from tests.utils import get_test_storage_root
from tests.workspace.utils import EMPTY_WORKSPACE_DIR

INIT_REPO_LOCATION = DEFAULT_VERIFIED_SOURCES_REPO
INIT_REPO_BRANCH = "master"
WORKSPACE_CLI_CASES_DIR = os.path.abspath(os.path.join("tests", "workspace", "cli", "cases"))


@pytest.fixture(autouse=True)
def auto_echo_default_choice(reset_echo_state: None, monkeypatch: pytest.MonkeyPatch) -> None:
    """Force `ALWAYS_CHOOSE_DEFAULT=True` for tests that import this fixture, on top of the conftest reset."""
    monkeypatch.setattr(echo, "ALWAYS_CHOOSE_DEFAULT", True)


@pytest.fixture(scope="session")
def _cached_init_repo(tmp_path_factory) -> FileStorage:
    cache_dir = tmp_path_factory.mktemp("cached_verified_sources_repo")
    return git.get_fresh_repo_files(
        INIT_REPO_LOCATION,
        cache_dir,
        branch=INIT_REPO_BRANCH,
    )


@pytest.fixture
def cloned_init_repo(_cached_init_repo: FileStorage) -> FileStorage:
    target = os.path.join(
        get_dlt_repos_dir(),
        f"verified_sources_repo_{uniq_id()}",
    )
    shutil.copytree(_cached_init_repo.storage_path, target)
    return FileStorage(target)


@pytest.fixture
def repo_dir(cloned_init_repo: FileStorage) -> Iterator[str]:
    dir_ = get_repo_dir(cloned_init_repo, f"verified_sources_repo_{uniq_id()}")

    prev_modules = set(sys.modules.keys())
    try:
        yield dir_
    finally:
        print(
            "NEWE MODULES",
            [
                getattr(sys.modules[mod], "__file__", None)
                for mod in set(sys.modules.keys()).difference(prev_modules)
            ],
        )
        # drop sys.modules entries loaded from this repo dir so the next test
        # re-executes their @dlt.source/@dlt.resource decorators and re-registers
        # sources in `SourceReference.SOURCES` after `workspace_files` clears it
        # _remove_modules(get_test_storage_root())


@pytest.fixture
def workspace_files() -> Iterator[FileStorage]:
    workspace_files = get_workspace_files()
    yield workspace_files


def get_repo_dir(cloned_repo: FileStorage, repo_name: str) -> str:
    # Create repo dir relative to current working directory
    repo_dir = os.path.join(get_test_storage_root(), repo_name)
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

    # project dir - use current working directory
    return FileStorage(EMPTY_WORKSPACE_DIR, makedirs=False)
