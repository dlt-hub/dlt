import os
import tempfile
import giturlparse
from typing import Iterator, Optional, TYPE_CHECKING
from contextlib import contextmanager

from dlt.common.storages import FileStorage
from dlt.common.utils import uniq_id
from dlt.common.typing import Any


# NOTE: never import git module directly as it performs a check if the git command is available and raises ImportError
if TYPE_CHECKING:
    from git import Repo
else:
    Repo = Any

@contextmanager
def git_custom_key_command(private_key: Optional[str]) -> Iterator[str]:
    if private_key:
        key_file = tempfile.mktemp(prefix=uniq_id())
        with open(key_file, "w", encoding="utf-8") as f:
            f.write(private_key)
        try:
            # permissions so SSH does not complain
            os.chmod(key_file, 0o600)
            yield 'ssh -o "StrictHostKeyChecking accept-new" -i "%s"' % key_file.replace("\\", "\\\\")
        finally:
            os.remove(key_file)
    else:
        yield 'ssh -o "StrictHostKeyChecking accept-new"'


def is_clean_and_synced(repo: Repo) -> bool:
    """Checks if repo is clean and synced with origin"""
    # get branch status
    status: str = repo.git.status("--short", "--branch")
    # we expect first status line ## main...origin/main
    status_lines = status.splitlines()
    first_line = status_lines[0]
    # we expect first status line is not ## main...origin/main [ahead 1]
    return len(status_lines) == 1 and first_line.startswith("##") and not first_line.endswith("]")


def is_dirty(repo: Repo) -> bool:
    status: str = repo.git.status("--short")
    return len(status.strip()) > 0

# def is_dirty(repo: Repo) -> bool:
#     # get branch status
#     status: str = repo.git.status("--short", "--branch")
#     # we expect first status line ## main...origin/main
#     return len(status.splitlines()) > 1


def ensure_remote_head(repo_path: str, branch: Optional[str] = None, with_git_command: Optional[str] = None) -> None:
    from git import Repo, RepositoryDirtyError

    # update remotes and check if heads are same. ignores locally modified files
    with Repo(repo_path) as repo:
        # use custom environment if specified
        with repo.git.custom_environment(GIT_SSH_COMMAND=with_git_command):
            # checkout branch before fetching
            if branch:
                repo.git.checkout(branch)
            # update origin
            repo.remote().pull()
            if not is_clean_and_synced(repo):
                status: str = repo.git.status("--short", "--branch")
                raise RepositoryDirtyError(repo, status)


def clone_repo(repository_url: str, clone_path: str, branch: Optional[str] = None, with_git_command: Optional[str] = None) -> Repo:
    from git import Repo

    repo = Repo.clone_from(repository_url, clone_path, env=dict(GIT_SSH_COMMAND=with_git_command))
    if branch:
        repo.git.checkout(branch)
    return repo


def force_clone_repo(repo_url: str, repo_storage: FileStorage, repo_name: str, branch: Optional[str] = None, with_git_command: Optional[str] = None) -> None:
    """Deletes the working directory repo_storage.root/repo_name and clones the `repo_url` into it. Will checkout `branch` if provided"""
    try:
        # delete repo folder
        if repo_storage.has_folder(repo_name):
            repo_storage.delete_folder(repo_name, recursively=True, delete_ro=True)
        clone_repo(
            repo_url,
            repo_storage.make_full_path(repo_name),
            branch=branch,
            with_git_command=with_git_command
        ).close()
    except Exception:
        # delete folder so we start clean next time
        if repo_storage.has_folder(repo_name):
            repo_storage.delete_folder(repo_name, recursively=True, delete_ro=True)
        raise


def get_fresh_repo_files(repo_location: str, working_dir: str = None, branch: Optional[str] = None, with_git_command: Optional[str] = None) -> FileStorage:
    """Returns a file storage leading to the newest repository files. If `repo_location` is url, file will be checked out into `working_dir/repo_name`"""
    from git import GitError

    url = giturlparse.parse(repo_location, check_domain=False)
    if not url.valid:
        # repo is a directory so jus return storage
        return FileStorage(repo_location, makedirs=False)
    else:
        # clone or update repo
        repo_name = url.name
        repo_path = os.path.join(working_dir, repo_name)
        try:
            ensure_remote_head(repo_path, branch=branch, with_git_command=with_git_command)
        except GitError:
            force_clone_repo(repo_location, FileStorage(working_dir, makedirs=True), repo_name, branch=branch, with_git_command=with_git_command)
        return FileStorage(repo_path)


def get_repo(path: str) -> Repo:
    from git import Repo

    repo = Repo(path, search_parent_directories=True)
    return repo


def get_origin(repo: Repo) -> str:
    return repo.remote().url
