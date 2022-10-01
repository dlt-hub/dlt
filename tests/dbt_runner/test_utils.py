import os
import shutil
from git import GitCommandError, Repo, RepositoryDirtyError
import pytest

from dlt.common.storages import FileStorage

from dlt.dbt_runner.utils import DBTProcessingError, clone_repo, ensure_remote_head, git_custom_key_command, initialize_dbt_logging, run_dbt_command

from tests.utils import test_storage
from tests.dbt_runner.utils import load_secret, modify_and_commit_file, restore_secret_storage_path


AWESOME_REPO = "https://github.com/sindresorhus/awesome.git"
JAFFLE_SHOP_REPO = "https://github.com/dbt-labs/jaffle_shop.git"
PRIVATE_REPO = "git@github.com:scale-vector/rasa_bot_experiments.git"
PRIVATE_REPO_WITH_ACCESS = "git@github.com:scale-vector/test_private_repo.git"


def test_ssh_key_context() -> None:
    secret = load_secret("deploy_key")
    with git_custom_key_command(secret) as git_command:
        assert len(git_command) > 0
        file_path = git_command.split(" ")[-1]
        assert os.path.isfile(file_path)
    # deleted out of context
    assert not os.path.isfile(file_path)


def test_no_ssh_key_context() -> None:
    with git_custom_key_command(None) as git_command:
        assert git_command == 'ssh -o "StrictHostKeyChecking accept-new"'


def test_clone(test_storage: FileStorage) -> None:
    repo_path = test_storage.make_full_path("awesome_repo")
    # clone a small public repo
    clone_repo(AWESOME_REPO, repo_path, with_git_command=None)
    assert test_storage.has_folder("awesome_repo")
    # make sure directory clean
    ensure_remote_head(repo_path, with_git_command=None)


def test_clone_with_commit_id(test_storage: FileStorage) -> None:
    repo_path = test_storage.make_full_path("awesome_repo")
    # clone a small public repo
    clone_repo(AWESOME_REPO, repo_path, with_git_command=None, branch="7f88000be2d4f265c83465fec4b0b3613af347dd")
    assert test_storage.has_folder("awesome_repo")
    ensure_remote_head(repo_path, with_git_command=None)


def test_clone_with_wrong_branch(test_storage: FileStorage) -> None:
    repo_path = test_storage.make_full_path("awesome_repo")
    # clone a small public repo
    with pytest.raises(GitCommandError):
        clone_repo(AWESOME_REPO, repo_path, with_git_command=None, branch="wrong_branch")


def test_clone_with_deploy_key_access_denied(test_storage: FileStorage) -> None:
    secret = load_secret("deploy_key")
    repo_path = test_storage.make_full_path("private_repo")
    with git_custom_key_command(secret) as git_command:
        with pytest.raises(GitCommandError):
            clone_repo(PRIVATE_REPO, repo_path, with_git_command=git_command)


def test_clone_with_deploy_key(test_storage: FileStorage) -> None:
    secret = load_secret("deploy_key")
    repo_path = test_storage.make_full_path("private_repo_access")
    with git_custom_key_command(secret) as git_command:
        clone_repo(PRIVATE_REPO_WITH_ACCESS, repo_path, with_git_command=git_command)
        ensure_remote_head(repo_path, with_git_command=git_command)


def test_repo_status_update(test_storage: FileStorage) -> None:
    secret = load_secret("deploy_key")
    repo_path = test_storage.make_full_path("private_repo_access")
    with git_custom_key_command(secret) as git_command:
        clone_repo(PRIVATE_REPO_WITH_ACCESS, repo_path, with_git_command=git_command)
        # modify README.md
        readme_path = modify_and_commit_file(repo_path, "README.md")
        assert test_storage.has_file(readme_path)
        with pytest.raises(RepositoryDirtyError):
            ensure_remote_head(repo_path, with_git_command=git_command)


def test_dbt_commands(test_storage: FileStorage) -> None:
    repo_path = test_storage.make_full_path("jaffle_shop")
    # clone jaffle shop for dbt 1.0.0
    clone_repo(JAFFLE_SHOP_REPO, repo_path, with_git_command=None, branch="core-v1.0.0")
    # copy profile
    shutil.copy("./tests/dbt_runner/cases/profiles.yml", repo_path)
    # initialize logging
    global_args = initialize_dbt_logging("INFO", False)
    # run deps, results are None
    assert run_dbt_command(repo_path, "deps", ".", global_args=global_args) is None
    # profiles in cases require this var to be set
    dbt_vars = {"dbt_schema": "JM_EKS"}
    # run list, results are string
    results = run_dbt_command(repo_path, "list", ".", global_args=global_args, dbt_vars=dbt_vars)
    assert len(results) == 28
    assert "jaffle_shop.not_null_orders_amount" in results
    # run list for specific selector
    results = run_dbt_command(repo_path, "list", ".", global_args=global_args, command_args=["-s", "jaffle_shop.not_null_orders_amount"], dbt_vars=dbt_vars)
    assert len(results) == 1
    assert results[0] == "jaffle_shop.not_null_orders_amount"
    # run debug, that will fail
    with pytest.raises(DBTProcessingError) as dbt_err:
        run_dbt_command(repo_path, "debug", ".", global_args=global_args, dbt_vars=dbt_vars)
    # results are bool
    assert dbt_err.value.command == "debug"

    # we have no database connectivity so tests will fail
    with pytest.raises(DBTProcessingError) as dbt_err:
        run_dbt_command(repo_path, "test", ".", global_args=global_args, dbt_vars=dbt_vars)
    # in that case test results are bool, not list of tests runs
    assert dbt_err.value.command == "test"

    # same for run
    with pytest.raises(DBTProcessingError) as dbt_err:
        run_dbt_command(repo_path, "run", ".", global_args=global_args, dbt_vars=dbt_vars, command_args=["--fail-fast", "--full-refresh"])
    # in that case test results are bool, not list of tests runs
    assert dbt_err.value.command == "run"
