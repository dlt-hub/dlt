import os
from git import GitCommandError, RepositoryDirtyError, GitError
import pytest

from dlt.common.storages import FileStorage
from dlt.common.git import (
    clone_repo,
    ensure_remote_head,
    git_custom_key_command,
    get_fresh_repo_files,
    get_repo,
    is_dirty,
    is_clean_and_synced,
)

from tests.utils import test_storage, skipifwindows
from tests.common.utils import (
    load_secret,
    modify_and_commit_file,
    restore_secret_storage_path,
)


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
    clone_repo(AWESOME_REPO, repo_path, with_git_command=None).close()
    assert test_storage.has_folder("awesome_repo")
    # make sure directory clean
    ensure_remote_head(repo_path, with_git_command=None)


def test_clone_with_commit_id(test_storage: FileStorage) -> None:
    repo_path = test_storage.make_full_path("awesome_repo")
    # clone a small public repo
    clone_repo(
        AWESOME_REPO,
        repo_path,
        with_git_command=None,
        branch="7f88000be2d4f265c83465fec4b0b3613af347dd",
    ).close()
    assert test_storage.has_folder("awesome_repo")
    # cannot pull detached head
    with pytest.raises(GitError):
        ensure_remote_head(repo_path, with_git_command=None)


def test_clone_with_wrong_branch(test_storage: FileStorage) -> None:
    repo_path = test_storage.make_full_path("awesome_repo")
    # clone a small public repo
    with pytest.raises(GitCommandError):
        clone_repo(
            AWESOME_REPO, repo_path, with_git_command=None, branch="wrong_branch"
        )


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
        clone_repo(
            PRIVATE_REPO_WITH_ACCESS, repo_path, with_git_command=git_command
        ).close()
        ensure_remote_head(repo_path, with_git_command=git_command)


@skipifwindows
def test_repo_status_update(test_storage: FileStorage) -> None:
    secret = load_secret("deploy_key")
    repo_path = test_storage.make_full_path("private_repo_access")
    with git_custom_key_command(secret) as git_command:
        clone_repo(
            PRIVATE_REPO_WITH_ACCESS, repo_path, with_git_command=git_command
        ).close()
        # modify README.md
        readme_path, _ = modify_and_commit_file(repo_path, "README.md")
        assert test_storage.has_file(readme_path)
        with pytest.raises(RepositoryDirtyError):
            ensure_remote_head(repo_path, with_git_command=git_command)


def test_fresh_repo_files_branch_change(test_storage: FileStorage) -> None:
    repo_storage = get_fresh_repo_files(
        AWESOME_REPO, test_storage.storage_path, branch="gh-pages"
    )
    with get_repo(repo_storage.storage_path) as repo:
        assert repo.active_branch.name == "gh-pages"
        assert not is_dirty(repo)
        assert is_clean_and_synced(repo)
    # change to main
    repo_storage = get_fresh_repo_files(
        AWESOME_REPO, test_storage.storage_path, branch="main"
    )
    with get_repo(repo_storage.storage_path) as repo:
        assert repo.active_branch.name == "main"
        assert not is_dirty(repo)
        assert is_clean_and_synced(repo)


def test_refresh_repo_files_local_mod(test_storage: FileStorage) -> None:
    repo_storage = get_fresh_repo_files(
        AWESOME_REPO, test_storage.storage_path, branch="main"
    )
    with get_repo(repo_storage.storage_path) as repo:
        origin_head_sha = repo.head.commit.hexsha
        repo_storage.save("addition.py", "# new file")
        assert is_dirty(repo)
        assert not is_clean_and_synced(repo)
        repo.index.add("addition.py")
        commit = repo.index.commit("mod mod mod")
        assert commit.hexsha == repo.head.commit.hexsha
        assert not is_dirty(repo)
        assert not is_clean_and_synced(repo)
    # get repo as local folder
    repo_storage = get_fresh_repo_files(repo_storage.storage_path, None)
    with get_repo(repo_storage.storage_path) as repo:
        # we are in non-synced state, folder does not refresh not clean
        assert commit.hexsha == repo.head.commit.hexsha
    # this should reset to the origin
    repo_storage = get_fresh_repo_files(
        AWESOME_REPO, test_storage.storage_path, branch="main"
    )
    with get_repo(repo_storage.storage_path) as repo:
        assert origin_head_sha == repo.head.commit.hexsha


def test_refresh_repo_files_ff(test_storage: FileStorage) -> None:
    repo_storage = get_fresh_repo_files(AWESOME_REPO, test_storage.storage_path)
    with get_repo(repo_storage.storage_path) as repo:
        origin_head_sha = repo.head.commit.hexsha
        # check out abc8bb09858130d6867121c74864ec00ef4d531f
        repo.git.reset("abc8bb09858130d6867121c74864ec00ef4d531f", "--hard")
        assert repo.head.commit.hexsha == "abc8bb09858130d6867121c74864ec00ef4d531f"
    # fast forward
    repo_storage = get_fresh_repo_files(AWESOME_REPO, test_storage.storage_path)
    with get_repo(repo_storage.storage_path) as repo:
        assert origin_head_sha == repo.head.commit.hexsha


def test_refresh_repo_files_conflict(test_storage: FileStorage) -> None:
    repo_storage = get_fresh_repo_files(AWESOME_REPO, test_storage.storage_path)
    with get_repo(repo_storage.storage_path) as repo:
        origin_head_sha = repo.head.commit.hexsha
        # check out abc8bb09858130d6867121c74864ec00ef4d531f
        repo.git.reset("abc8bb09858130d6867121c74864ec00ef4d531f", "--hard")
        assert repo.head.commit.hexsha == "abc8bb09858130d6867121c74864ec00ef4d531f"
        # modify detached head
        repo_storage.save("readme.md", "# new file")
        repo.index.add("readme.md")
        repo.index.commit("mod mod mod")
    # this should reset to the origin
    repo_storage = get_fresh_repo_files(AWESOME_REPO, test_storage.storage_path)
    with get_repo(repo_storage.storage_path) as repo:
        assert origin_head_sha == repo.head.commit.hexsha
