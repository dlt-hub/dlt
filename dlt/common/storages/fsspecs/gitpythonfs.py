from typing import List, Dict, Any, Optional, Union
from os import PathLike
from git import Repo
from git.objects import Blob, Tree, Object
from fsspec.registry import register_implementation
from fsspec.spec import AbstractFileSystem
from fsspec.implementations.memory import MemoryFile
from functools import lru_cache


def register_implementation_in_fsspec() -> None:
    """Dyanmically register the filesystem with fsspec.

    This is needed if the implementation is not officially registered in the fsspec codebase.
    It will also override ("clobber") an existing implementation having the same protocol.
    The registration's scope is the current process.
    """
    register_implementation(
        "gitpythonfs",
        "dlt.common.storages.fsspecs.gitpythonfs.GitPythonFileSystem",
        clobber=True,
        errtxt="Please install gitpythonfs to access GitPythonFileSystem",
    )


# Thanks to other implemetnation authors: https://github.com/fsspec/filesystem_spec/tree/master/fsspec/implementations
class GitPythonFileSystem(AbstractFileSystem):
    """A filesystem for git repositories on the local filesystem.

    An instance of this class provides the files residing within a local git
    repository. You may specify a point in the repo's history, by SHA, branch
    or tag (default is HEAD).

    You can retrieve information such as a file's modified time, which would not
    be possible if looking at the local filesystem directly.

    It is based on the gitpython library, which could also be used to clone or update
    files from a remote repo before reading them with this filesystem.

    Instances of this class cache some git objects so it not recommended to change
    the git repo within the lifetime of an instance. Calling clear_git_caches() may help.
    Also note that fsspec itself caches entire instances, which can be overridden with
    the `skip_instance_cache=True` keyword argument.
    """

    protocol = "gitpythonfs"
    READ_ONLY_MESSAGE = "This fsspec implementation is read-only."

    def __init__(
        self, repo_path: Optional[str] = None, ref: Optional[str] = None, **kwargs: Any
    ) -> None:
        """
        Initialize a GitPythonFS object.

        In fucntions such as open_files() and url_to_fs(), arguments can
        be passed in the url instead of as keyword parameters:
        `gitpythonfs://<path-to-repo:><ref@>path/to/file`
        (though it doesn't escape [:@] or any other special characters).
        Examples:
            `gitpythonfs:///some_folder/my_repo:path/to/intro.md`
            `gitpythonfs:///some_folder/my_repo:mybranch@path/to/intro.md`

        Args:
            repo_path (str): Local location of the Git repo. Defaults to current directory.
            ref (str): A branch, tag or commit hash to use.
                Defaults to HEAD of the local repo.
        """
        super().__init__(**kwargs)

        self.repo_path = repo_path
        self.repo = Repo(self.repo_path)

        # error early if bad ref. ToDo: check this whenever ref passed to any method
        if ref:
            self.repo.commit(ref)

        self.ref = ref or self.repo.head.ref.name

        self._get_tree = lru_cache(maxsize=128)(self._get_tree_uncached)
        self._get_revision_details = lru_cache(maxsize=16)(self._get_revision_details_uncached)

    def _get_tree_uncached(self, ref: str) -> Tree:
        """Get the tree at repo root for a given ref

        Args:
            ref (str): The reference to the commit, branch or tag

        Returns:
            git.Tree: The tree object at the root of the repository for the given ref
        """
        return self.repo.tree(ref)

    def _get_revision_details_uncached(self, ref: str) -> Dict[str, int]:
        """Get the revisions at a given ref for entire repo including subdirectories.

        Args:
            ref (str): The reference to the commit, branch or tag

        Returns:
            Dict[str, int]: A dictionary mapping file path to file's last modified time
        """
        result: Dict[str, int] = parse_git_revlist(
            get_revisions_all_raw(self.repo, ref or self.ref)
        )
        return result

    def clear_git_caches(self) -> None:
        """Clear the git caches.

        This is useful if the repo has changed and you want to force a refresh of the
        cached objects. Also not that fsspec itself may cache instances of AbstractFileSystem.
        """
        self._get_tree.cache_clear()
        self._get_revision_details.cache_clear()

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        path = super()._strip_protocol(path).lstrip("/")
        if ":" in path:
            path = path.split(":", 1)[1]
        if "@" in path:
            path = path.split("@", 1)[1]
        return path.lstrip("/")

    @staticmethod
    def _get_kwargs_from_urls(path: str) -> Dict[str, str]:
        if path.startswith("gitpythonfs://"):
            path = path[14:]
        out = {}
        if ":" in path:
            out["repo_path"], path = path.split(":", 1)
        if "@" in path:
            out["ref"], path = path.split("@", 1)
        return out

    def _git_type_to_file_type(self, git_object: Object) -> str:
        if isinstance(git_object, Blob):
            return "file"
        elif isinstance(git_object, Tree):
            return "directory"
        else:
            msg = (
                "There is no fileystem object type corresponding to Git object type:"
                f" {type(git_object).__name__}"
            )
            raise TypeError(msg)

    def _details(
        self,
        git_object: Object,
        ref: Optional[str] = None,
        include_committed_date: bool = True,
    ) -> Dict[str, Union[str, int]]:
        """
        Retrieves the details of a Git object.

        Args:
            object (git.Object): The Git object to retrieve details for.
            include_committed_date (bool, optional): Whether to include the committed date. Defaults to True.
                Getting the committed date is an expensive operation and will slow down
                walk(), a method that is extensively used by fsspec for find(), glob() etc.

        Returns:
            dict: A dictionary containing the details typical for fsspec.
        """
        object_type: str = self._git_type_to_file_type(git_object)

        details = {
            "name": git_object.path,
            "type": self._git_type_to_file_type(git_object),
            "mode": f"{git_object.mode:o}",
            "mime_type": git_object.mime_type if object_type == "file" else None,
            "size": git_object.size,
            "hex": git_object.hexsha,
        }

        # Only get expensive details if needed.
        if object_type == "file" and include_committed_date:
            details["mtime"] = self._get_revision_details(ref or self.ref)[git_object.path]

        return details

    def ls(
        self,
        path: str,
        detail: bool = False,
        ref: Optional[str] = None,
        **kwargs: Any,
    ) -> List[Any]:
        """List files at given path in the repo."""
        path = self._strip_protocol(path)
        results: List[Any] = []

        # GitPython recommends always starting at root of repo.
        tree = self._get_tree(ref or self.ref)

        object_at_path = tree if path == "" else tree / path
        if isinstance(object_at_path, Tree):
            if detail:
                for git_object in object_at_path:
                    results.append(self._details(git_object, ref or self.ref, **kwargs))
                return results
            else:
                for git_object in object_at_path:
                    results.append(git_object.path)
                return results
        else:
            # path is to a single blob.
            if detail:
                results.append(self._details(object_at_path, ref or self.ref, **kwargs))
                return results
            else:
                results.append(object_at_path.path)
                return results

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: Optional[int] = None,
        autocommit: bool = True,
        cache_options: Optional[Any] = None,
        ref: Optional[str] = None,
        **kwargs: Any,
    ) -> MemoryFile:
        path = self._strip_protocol(path)
        tree = self._get_tree(ref or self.ref)
        blob = tree / path
        return MemoryFile(data=blob.data_stream.read())

    def mv(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(self.READ_ONLY_MESSAGE)

    def rm(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(self.READ_ONLY_MESSAGE)

    def touch(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(self.READ_ONLY_MESSAGE)

    def mkdir(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(self.READ_ONLY_MESSAGE)

    def mkdirs(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(self.READ_ONLY_MESSAGE)

    def rmdir(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(self.READ_ONLY_MESSAGE)

    def put_file(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(self.READ_ONLY_MESSAGE)

    def put(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(self.READ_ONLY_MESSAGE)

    def cp_file(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(self.READ_ONLY_MESSAGE)

    def copy(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(self.READ_ONLY_MESSAGE)

    def rm_file(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(self.READ_ONLY_MESSAGE)

    def _rm(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(self.READ_ONLY_MESSAGE)

    def chmod(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(self.READ_ONLY_MESSAGE)

    def chown(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(self.READ_ONLY_MESSAGE)


def get_revisions_all_raw(repo: Repo, ref: str) -> str:
    """
    Get the git revisions at a given ref for entire repo including subdirectories.

    Is much faster than iterating commits in GitPython.

    Args:
        repo (git.Repo): The git repository object.
        ref (str): The reference (commit, branch, tag, etc.) to get the revisions from.

    Returns:
        str: The revisions at the given ref, as would be on stdout".
    """

    git_cmd_runner = repo.git

    # git uses fnmatch(3) style matching
    path_spec = ":(top)"

    out: str = git_cmd_runner.log(ref, path_spec, raw=True, no_merges=True, pretty="%at")
    return out


def parse_git_revlist(git_cmd_output: str) -> Dict[str, int]:
    """
    Parse raw text output produced by git rev-list

    `git log` and `git whatchanged` have similar output.

    Args:
        raw_text (str): The git cmd output to parse.

    Returns:
        dict: The parsed revisions info as a dictionary with:
            key: path to file
            value: most recent commit timestamp for file
    """
    revisions_info = {}

    lines = git_cmd_output.splitlines()
    for line in lines:
        line = line.strip()
        if not line:
            continue

        if not line.startswith(":"):
            # it's the line with the pretty format info
            committed_at = int(line)
        else:
            filepath = line.split("\t")[-1]
            # git outputs revisions newest first. So we ignore a files we've
            # already seen it because we only want metadata for the latest
            # commit for each file.
            if filepath not in revisions_info:
                revisions_info[filepath] = committed_at

    return revisions_info


def get_revisions_all(repo: Repo, ref: str) -> Dict[str, int]:
    """Example of getting revisions raw output and parsing it."""

    raw = get_revisions_all_raw(repo, ref)
    return parse_git_revlist(raw)
