import os
from git import GitCommandError, RepositoryDirtyError
import pytest

from dlt.common.storages import FileStorage
from dlt.common.git import clone_repo, ensure_remote_head, git_custom_key_command

from tests.utils import test_storage, skipifwindows
from tests.common.utils import load_secret, modify_and_commit_file, restore_secret_storage_path


AWESOME_REPO = "https://github.com/sindresorhus/awesome.git"
JAFFLE_SHOP_REPO = "https://github.com/dbt-labs/jaffle_shop.git"
PRIVATE_REPO = "git@github.com:scale-vector/rasa_bot_experiments.git"
PRIVATE_REPO_WITH_ACCESS = "git@github.com:scale-vector/test_private_repo.git"


def test_ssh_key_context() -> None:
    secret = load_secret("deploy_key")
    with git_custom_key_command(secret) as git_command:
        assert len(git_command) > 0
        file_path = git_command.split(" ")[-1]
        assert os.path.isfile(file_path.strip('"'))
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


@skipifwindows
def test_clone_with_deploy_key(test_storage: FileStorage) -> None:
    secret = load_secret("deploy_key")
    repo_path = test_storage.make_full_path("private_repo_access")
    with git_custom_key_command(secret) as git_command:
        clone_repo(PRIVATE_REPO_WITH_ACCESS, repo_path, with_git_command=git_command)
        ensure_remote_head(repo_path, with_git_command=git_command)


@skipifwindows
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