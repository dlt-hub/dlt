import os
import tempfile
from typing import Iterator, Optional
from git import Repo, RepositoryDirtyError
from contextlib import contextmanager

from dlt.common.utils import uniq_id


@contextmanager
def git_custom_key_command(private_key: Optional[str]) -> Iterator[str]:
    if private_key:
        key_file = tempfile.mktemp(prefix=uniq_id())
        with open(key_file, "w", encoding="utf-8") as f:
            f.write(private_key)
        try:
            # permissions so SSH does not complain
            os.chmod(key_file, 0o600)
            yield 'ssh -o "StrictHostKeyChecking accept-new" -i %s' % key_file
        finally:
            os.remove(key_file)
    else:
        yield 'ssh -o "StrictHostKeyChecking accept-new"'


def ensure_remote_head(repo_path: str, with_git_command: Optional[str] = None) -> None:
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


def clone_repo(repository_url: str, clone_path: str, branch: Optional[str] = None, with_git_command: Optional[str] = None) -> None:
    repo = Repo.clone_from(repository_url, clone_path, env=dict(GIT_SSH_COMMAND=with_git_command))
    if branch:
        repo.git.checkout(branch)
