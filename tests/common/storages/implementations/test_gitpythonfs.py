import os
import subprocess
import tempfile
import shutil

import pytest
from typing import Iterator, Any

import fsspec
from fsspec.registry import (
    known_implementations,
    available_protocols,
    filesystem,
    register_implementation,
)

from git import Repo, BadName

from dlt.common.storages.implementations.gitpythonfs import (
    GitPythonFileSystem,
    register_implementation_in_fsspec,
    get_revisions_all_raw,
    parse_git_revlist,
)

PROTOCOL = GitPythonFileSystem.protocol
test_fs_kwargs = {"skip_instance_cache": True}


@pytest.fixture(autouse=True)
def ensure_fs_registered(request) -> Iterator[None]:
    """Ensure that the gitpythonfs implementation is registered in fsspec"""
    if "no_registration_fixture" in request.keywords:
        # skip registration for tests marked @pytest.mark.no_registration_fixture
        yield None
    else:
        try:
            if PROTOCOL in known_implementations:
                known_implementations.pop(PROTOCOL)
            register_implementation_in_fsspec()
            yield None
        finally:
            if PROTOCOL in known_implementations:
                known_implementations.pop(PROTOCOL)


@pytest.fixture()
def repo_fixture() -> Iterator[Any]:
    """Create a temporary git repository.

    Thanks to https://github.com/fsspec/filesystem_spec/blob/master/fsspec/implementations/tests/test_git.py
    """
    original_working_dir = os.getcwd()
    repo_path = tempfile.mkdtemp()
    try:
        os.chdir(repo_path)
        subprocess.call("git init", shell=True, cwd=repo_path)
        subprocess.call("git checkout -b master", shell=True, cwd=repo_path)
        subprocess.call("git config user.email 'you@example.com'", shell=True, cwd=repo_path)
        subprocess.call("git config user.name 'Your Name'", shell=True, cwd=repo_path)

        open(os.path.join(repo_path, "file1"), "w", encoding="utf-8").write("four5")
        subprocess.call("git add file1", shell=True, cwd=repo_path)
        subprocess.call("git commit -m 'init'", shell=True, cwd=repo_path)
        sha_first = (
            subprocess.check_output("git rev-parse HEAD", shell=True, cwd=repo_path)
            .strip()
            .decode("utf-8")
        )

        open(os.path.join(repo_path, "file1"), "w", encoding="utf-8").write("four56")
        subprocess.call("git commit -a -m 'tagger'", shell=True, cwd=repo_path)
        subprocess.call("git tag -a thetag -m 'make tag'", shell=True, cwd=repo_path)

        open(os.path.join(repo_path, "file2"), "w", encoding="utf-8").write("four567")
        subprocess.call("git add file2", shell=True)
        subprocess.call("git commit -m 'master tip'", shell=True, cwd=repo_path)

        subprocess.call("git checkout -b abranch", shell=True, cwd=repo_path)
        os.mkdir("inner")
        open(os.path.join(repo_path, "inner", "file3"), "w", encoding="utf-8").write("four5")
        subprocess.call("git add inner/file3", shell=True, cwd=repo_path)
        open(os.path.join(repo_path, "inner", "file4"), "w", encoding="utf-8").write("four5")
        subprocess.call("git add inner/file4", shell=True, cwd=repo_path)
        subprocess.call("git commit -m 'abranch tip'", shell=True, cwd=repo_path)

        os.chdir(original_working_dir)
        yield repo_path, sha_first
    finally:
        os.chdir(original_working_dir)
        shutil.rmtree(repo_path)


@pytest.mark.no_registration_fixture
def test_register_implementation_in_fsspec() -> None:
    """Test registering a filesystem implementation with fsspec.

    Takes care with state since other tests may be expecting certain
    implementations to be registered.
    """
    previous_registration_existed = False

    # setup
    if PROTOCOL in known_implementations:
        backup = known_implementations.pop(PROTOCOL)
        previous_registration_existed = True

    assert (
        PROTOCOL not in known_implementations
    ), f"As a test precondition, {PROTOCOL} should not be registered."

    # do and test
    register_implementation_in_fsspec()
    assert PROTOCOL in available_protocols(), f"{PROTOCOL} should be registered."

    # teardown
    if previous_registration_existed:
        register_implementation(PROTOCOL, backup, clobber=True)
        assert (
            PROTOCOL in available_protocols()
        ), f"After teardown, {PROTOCOL} should not be registered, which was the original state."
    else:
        known_implementations.pop(PROTOCOL)
        assert (
            PROTOCOL not in known_implementations
        ), f"After teardown, {PROTOCOL} should not be registered, which was the original state."


def test_instantiate_fsspec_filesystem(repo_fixture: Iterator[Any]) -> None:
    """Test instantiating a filesystem with fsspec."""
    d, _ = repo_fixture

    fs = filesystem(PROTOCOL, repo_path=d, **test_fs_kwargs)
    assert type(fs) == GitPythonFileSystem


def test_ls_entries(repo_fixture: Iterator[Any]) -> None:
    """Test listing folders and files in a repository."""
    d, _ = repo_fixture
    fs = filesystem(PROTOCOL, repo_path=d, **test_fs_kwargs)

    assert fs.ls("") == [
        "file1",
        "file2",
        "inner",
    ], "Should return all objects at root of repo."
    assert fs.ls("file1") == ["file1"], "Should return a single file at root."
    assert fs.ls("inner") == [
        "inner/file3",
        "inner/file4",
    ], "Should return 2 files, with their paths."
    assert fs.ls("inner/file3") == ["inner/file3"], "Should return a single file in folder."


def test_ls_file_details(repo_fixture: Iterator[Any]) -> None:
    """Test showing details for a file (git.Blob) in a repository."""

    # setup
    d, _ = repo_fixture
    fs = filesystem(PROTOCOL, repo_path=d, **test_fs_kwargs)

    # do
    files = fs.ls("file1", detail=True, include_committed_date=True)
    assert len(files) == 1, "Should return a single object."
    details = files[0]

    # assert
    assert details["name"] == "file1"
    assert details["type"] == "file"
    assert details["mime_type"] == "text/plain"
    assert isinstance(details["size"], int)
    assert isinstance(details["hex"], str)
    assert isinstance(
        details["mode"], str
    ), "Should be a string representation of octal, without the 0o prefix."
    assert isinstance(details["committed_date"], int)


def test_git_refs(repo_fixture: Iterator[Any]) -> None:
    """Test results for git refs - eg commit sha, branch, tag."""
    d, _ = repo_fixture

    with fsspec.open("gitpythonfs://inner/file3", repo_path=d, **test_fs_kwargs) as f:
        assert f.read() == b"four5", "Should read from HEAD if no ref given."

    try:
        with fsspec.open(
            "gitpythonfs://inner/file3", repo_path=d, ref="HEAD", **test_fs_kwargs
        ) as f:
            f.read()
    except BadName:
        pytest.fail("Should accept HEAD as a ref.")

    with pytest.raises(BadName):
        with fsspec.open(
            "gitpythonfs://file1",
            repo_path=d,
            ref="somenonexisentgitref",
            **test_fs_kwargs,
        ) as f:
            _ = f.read()


def test_git_refs_on_open(repo_fixture: Iterator[Any]) -> None:
    d, sha_first = repo_fixture

    with fsspec.open("gitpythonfs://file1", repo_path=d, ref=sha_first, **test_fs_kwargs) as f:
        assert f.read() == b"four5", "Should read file version at given sha (aka commit id)."

    with fsspec.open("gitpythonfs://file1", repo_path=d, ref="thetag", **test_fs_kwargs) as f:
        assert f.read() == b"four56", "Should read file version at given tag."


def test_git_refs_on_ls(repo_fixture: Iterator[Any]) -> None:
    d, sha_first = repo_fixture

    fs = filesystem(PROTOCOL, repo_path=d, ref=sha_first, **test_fs_kwargs)
    files = fs.ls("file1", detail=True)
    assert len(files) == 1, "Should return a single object."
    assert files[0]["size"] == 5, "Should return file size as at the sha given in constructor."

    fs = filesystem(PROTOCOL, repo_path=d, **test_fs_kwargs)
    files = fs.ls("file1", ref=sha_first, detail=True)
    assert len(files) == 1, "Should return a single object."
    assert files[0]["size"] == 5, "Should return file size as at sha given in ls()."

    fs = filesystem(PROTOCOL, repo_path=d, ref="HEAD", **test_fs_kwargs)
    files = fs.ls("file1", ref=sha_first, detail=True)
    assert len(files) == 1, "Should return a single object."
    assert files[0]["size"] == 5, "ls() ref should override constructor ref."


def test_get_kwargs_from_urls() -> None:
    """Test getting kwargs from url."""
    repo_path = "/some/path/to/repo"
    ref = "some_tag"

    url = f"gitpythonfs://{repo_path}:{ref}@file1"
    kwargs = GitPythonFileSystem._get_kwargs_from_urls(url)
    assert kwargs["repo_path"] == repo_path
    assert kwargs["ref"] == ref


def test_url(repo_fixture: Iterator[Any]) -> None:
    """Test reading a file from a repository via url.

    For supported url formats see GitPytonFileSystem class doco"""

    d, sha_first = repo_fixture

    with fsspec.open("gitpythonfs://file1", repo_path=d, **test_fs_kwargs) as f:
        assert f.read() == b"four56", "Should return file at root."

    with fsspec.open(f"gitpythonfs://{d}:file1", **test_fs_kwargs) as f:
        assert f.read() == b"four56", "Should return file via the repo path embedded in the url."

    with fsspec.open(f"gitpythonfs://{d}:{sha_first}@file1", **test_fs_kwargs) as f:
        assert (
            f.read() == b"four5"
        ), "Should return file via the repo path and git ref embedded in the url."


def test_multiple_files(repo_fixture: Iterator[Any]) -> None:
    """Test reading multiple files from a repository."""
    d, _ = repo_fixture

    files = fsspec.open_files(f"gitpythonfs://{d}:**file*", **test_fs_kwargs)
    assert len(files) == 4, "Glob should recurse folders and return 4 files that start with `file`."


def test_non_readonly_raises_exception(repo_fixture: Iterator[Any]) -> None:
    """Test that non-readonly operations raise an exception."""
    d, _ = repo_fixture

    with pytest.raises(NotImplementedError):
        GitPythonFileSystem(d, **test_fs_kwargs).mv()  # type: ignore


def test_get_revisions_all_raw(repo_fixture: Iterator[Any]) -> None:
    """Test getting all revisions."""
    d, _ = repo_fixture
    repo = Repo(d)

    result = get_revisions_all_raw(repo, "HEAD")

    assert not result == "", "Should return some info"
    assert all(
        filename in result for filename in ["file1", "file2", "inner/file3", "inner/file4"]
    ), "Should return info for all files in repo"


def test_get_revisions_all_raw_at_ref(repo_fixture: Iterator[Any]) -> None:
    """Test getting all revisions at ref."""
    d, sha_first = repo_fixture
    repo = Repo(d)

    result = get_revisions_all_raw(repo, sha_first)

    assert "file1" in result, "Should return info for one file that exists at ref"
    assert "file2" not in result, "Should not return info for file not existent at ref"


def test_parse_git_revlist() -> None:
    """Test parsing git revlist output."""

    git_cmd_output = (
        # for clarity, broken into neighbouring string constants, which Python
        # automatically concatenates again to be like the original.
        "1703550238\n"
        "\n"
        ":000000 100644 0000000 cdca2c1 A\tinner/file3\n"
        ":000000 100644 0000000 488d13d A\tinner/file4\n"
        "1703550237\n\n:000000 100644 0000000 aff88ce A\tfile2\n"
        "1703550236\n"
        "\n"
        ":100644 100644 1c37a15 a906852 M\tfile1\n"
        "1703550235\n"
        "\n"
        ":000000 100644 0000000 1c37a15 A\tfile1"
    )

    result = parse_git_revlist(git_cmd_output)
    assert result == {
        "inner/file3": 1703550238,
        "inner/file4": 1703550238,
        "file2": 1703550237,
        "file1": 1703550236,
    }, "Should return dict with latest timetamp for each file"
