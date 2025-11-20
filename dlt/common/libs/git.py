import os
import tempfile
import giturlparse
from typing import Iterator, Optional, TYPE_CHECKING
from contextlib import contextmanager

from dlt.common.exceptions import MissingDependencyException
from dlt.common.storages import FileStorage
from dlt.common.utils import uniq_id
from dlt.common.typing import Any


# NOTE: never import git module directly as it performs a check if the git command is available and raises ImportError
if TYPE_CHECKING:
    from git import Repo
else:
    Repo = Any


def _import_git() -> None:
    try:
        import git
    except ModuleNotFoundError:
        raise MissingDependencyException(
            "git repository helpers",
            ["gitpython>=3.1.29"],
            "Install PythonGit to work with git repositories.",
        )


@contextmanager
def git_custom_key_command(private_key: Optional[str]) -> Iterator[str]:
    if private_key:
        key_file = tempfile.mktemp(prefix=uniq_id())
        with open(key_file, "w", encoding="utf-8") as f:
            f.write(private_key)
        try:
            # permissions so SSH does not complain
            os.chmod(key_file, 0o600)
            yield 'ssh -o "StrictHostKeyChecking accept-new" -i "%s"' % key_file.replace(
                "\\", "\\\\"
            )
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


def get_default_branch(repo: Repo) -> str:
    origin = repo.remotes.origin
    # Get the remote's HEAD reference (default branch)
    default_ref = origin.refs.HEAD  # symbolic ref like 'origin/main'
    return default_ref.reference.name.split("/")[-1]  # extract branch


def ensure_remote_head(
    repo_path: str,
    branch: Optional[str] = None,
    with_git_command: Optional[str] = None,
    path: Optional[str] = None,
) -> None:
    """Updates repository from origin and ensures it's clean and synced.

    Uses sparse checkout when path is specified, fetching only the specified
    directory instead of the entire repository tree.

    Args:
        repo_path: Local path to the git repository.
        branch: Branch to checkout. Defaults to repository's default branch.
        with_git_command: Custom GIT_SSH_COMMAND for authentication.
        path: Directory path for sparse checkout. When set, only this path
            is checked out, reducing download size and time.

    Raises:
        RepositoryDirtyError: If repository has uncommitted changes or is
            not synced with origin.
    """
    from git import Repo, RepositoryDirtyError

    # update remotes and check if heads are same. ignores locally modified files
    with Repo(repo_path) as repo:
        # use custom environment if specified
        with repo.git.custom_environment(GIT_SSH_COMMAND=with_git_command):
            # if path is set, use sparse checkout
            if path is not None:
                # assume that sparse checkout was enabled when cloning
                repo.git.sparse_checkout("set", path)
            # checkout branch before fetching
            repo.git.checkout(branch or get_default_branch(repo))
            # update origin
            repo.remote().pull()
            if not is_clean_and_synced(repo):
                status: str = repo.git.status("--short", "--branch")
                raise RepositoryDirtyError(repo, status)


def clone_repo(
    repository_url: str,
    clone_path: str,
    branch: Optional[str] = None,
    with_git_command: Optional[str] = None,
    path: Optional[str] = None,
) -> Repo:
    _import_git()

    from git import Repo

    if path is not None:
        # set of options that prevents downloading all blobs
        multi_options = [
            "--depth=1",
            "--filter=blob:none",
            "--no-checkout",
        ]
    else:
        multi_options = None
    repo = Repo.clone_from(
        repository_url,
        clone_path,
        env=dict(GIT_SSH_COMMAND=with_git_command),
        multi_options=multi_options,
    )
    # set up sparse mode to checkout paths on demand
    if path is not None:
        repo.git.sparse_checkout("init", "--cone")
        repo.git.sparse_checkout("set", path)
    if branch:
        repo.git.checkout(branch)
    elif path is not None:
        repo.git.checkout()
    return repo


def force_clone_repo(
    repo_url: str,
    repo_storage: FileStorage,
    repo_name: str,
    branch: Optional[str] = None,
    with_git_command: Optional[str] = None,
    path: Optional[str] = None,
) -> None:
    """Deletes existing repository and performs fresh clone.

    Removes repo_storage.root/repo_name if it exists, then clones from
    repo_url. Uses sparse checkout when path is specified to download only
    the specified directory, reducing clone time and disk usage.

    Args:
        repo_url: Git repository URL to clone from.
        repo_storage: FileStorage instance managing the clone destination.
        repo_name: Directory name for the cloned repository.
        branch: Branch to checkout after cloning.
        with_git_command: Custom GIT_SSH_COMMAND for authentication.
        path: Directory path for sparse checkout. When set, only this path
            is cloned using --filter=blob:none and --depth=1.
    """
    try:
        # delete repo folder
        if repo_storage.has_folder(repo_name):
            repo_storage.delete_folder(repo_name, recursively=True, delete_ro=True)
        clone_repo(
            repo_url,
            repo_storage.make_full_path(repo_name),
            branch=branch,
            with_git_command=with_git_command,
            path=path,
        ).close()
    except Exception:
        # delete folder so we start clean next time
        if repo_storage.has_folder(repo_name):
            repo_storage.delete_folder(repo_name, recursively=True, delete_ro=True)
        raise


def get_fresh_repo_files(
    repo_location: str,
    working_dir: str = None,
    branch: Optional[str] = None,
    with_git_command: Optional[str] = None,
    path: Optional[str] = None,
) -> FileStorage:
    """Returns FileStorage with up-to-date repository files.

    If repo_location is a local directory, returns storage pointing to it.
    If it's a git URL, clones or updates the repository in working_dir/repo_name.
    Supports sparse checkout to fetch only a specific directory path.

    Args:
        repo_location: Local directory path or git repository URL.
        working_dir: Directory where repository will be cloned if repo_location is URL.
        branch: Branch to checkout.
        with_git_command: Custom GIT_SSH_COMMAND for authentication.
        path: Directory path for sparse checkout. Downloads only this directory,
            improving performance for large repositories.

    Returns:
        FileStorage instance pointing to repository files (or specified path within).
    """
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
            ensure_remote_head(
                repo_path, branch=branch, with_git_command=with_git_command, path=path
            )
        except GitError:
            force_clone_repo(
                repo_location,
                FileStorage(working_dir, makedirs=True),
                repo_name,
                branch=branch,
                with_git_command=with_git_command,
                path=path,
            )
        return FileStorage(repo_path)


def get_repo(path: str) -> Repo:
    _import_git()

    from git import Repo

    # if GIT_CEILING_DIRECTORIES is set then do not look up for repositories in parent dirs
    search_parent_directories: bool = True
    if os.getenv("GIT_CEILING_DIRECTORIES"):
        search_parent_directories = False

    repo = Repo(path, search_parent_directories=search_parent_directories)
    return repo


def get_origin(repo: Repo) -> str:
    return repo.remote().url
