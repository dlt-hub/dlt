import os
import tempfile
from typing import Iterator, Optional, TYPE_CHECKING
from contextlib import contextmanager

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


def ensure_remote_head(repo_path: str, with_git_command: Optional[str] = None) -> None:
    from git import Repo, RepositoryDirtyError

    # update remotes and check if heads are same. ignores locally modified files
    repo = Repo(repo_path)
    # use custom environment if specified
    with repo.git.custom_environment(GIT_SSH_COMMAND=with_git_command):
        # update origin
        repo.remote().update()
        # get branch status
        status: str = repo.git.status("--short", "--branch", "-uno")
        # we expect first status line ## main...origin/main
        status_line = status.split("/n")[0]
        if not (status_line.startswith("##") and not status_line.endswith("]")):
            raise RepositoryDirtyError(repo, status)


def clone_repo(repository_url: str, clone_path: str, branch: Optional[str] = None, with_git_command: Optional[str] = None) -> Repo:
    from git import Repo

    repo = Repo.clone_from(repository_url, clone_path, env=dict(GIT_SSH_COMMAND=with_git_command))
    if branch:
        repo.git.checkout(branch)
    return repo


def get_repo(path: str) -> Repo:
    from git import Repo

    repo = Repo(path, search_parent_directories=True)
    return repo


def get_origin(repo: Repo) -> str:
    return repo.remote().url


def is_repo_dirty(repo: Repo) -> bool:
    status: str = repo.git.status("--short")
    return len(status.strip()) > 0
