import gzip
import os
import stat
import pytest
from pathlib import Path
from typing import cast, TextIO

from dlt.common.storages.file_storage import FileStorage
from dlt.common.utils import encoding_for_mode, set_working_dir, uniq_id

from tests.utils import TEST_STORAGE_ROOT, autouse_test_storage, test_storage, skipifnotwindows


def test_storage_init(test_storage: FileStorage) -> None:
    # must be absolute path
    assert os.path.isabs(test_storage.storage_path)
    # may not contain file name (ends with / or \)
    assert os.path.basename(test_storage.storage_path) == TEST_STORAGE_ROOT

    # TODO: write more cases


def test_to_relative_path(test_storage: FileStorage) -> None:
    assert test_storage.to_relative_path(".") == "."
    assert test_storage.to_relative_path("") == ""
    assert test_storage.to_relative_path("a") == "a"
    assert test_storage.to_relative_path("a/b/c") == str(Path("a/b/c"))
    assert test_storage.to_relative_path("a/b/../..") == "."
    with pytest.raises(ValueError):
        test_storage.to_relative_path("a/b/../.././..")
    with pytest.raises(ValueError):
        test_storage.to_relative_path("../a/b/c")
    abs_path = os.path.join(test_storage.storage_path, "a/b/c")
    assert test_storage.to_relative_path(abs_path) == str(Path("a/b/c"))
    abs_path = os.path.join(test_storage.storage_path, "a/b/../c")
    assert test_storage.to_relative_path(abs_path) == str(Path("a/c"))


def test_make_full_path(test_storage: FileStorage) -> None:
    # fully within storage
    relative_path = os.path.join("dir", "to", "file")
    path = test_storage.make_full_path_safe(relative_path)
    assert path.endswith(os.path.join(TEST_STORAGE_ROOT, relative_path))
    # overlapped with storage
    root_path = os.path.join(TEST_STORAGE_ROOT, relative_path)
    path = test_storage.make_full_path_safe(root_path)
    assert path.endswith(root_path)
    assert path.count(TEST_STORAGE_ROOT) == 2
    # absolute path with different root than TEST_STORAGE_ROOT does not lead into storage so calculating full path impossible
    with pytest.raises(ValueError):
        test_storage.make_full_path_safe(os.path.join("/", root_path))
    # relative path out of the root
    with pytest.raises(ValueError):
        test_storage.make_full_path_safe("..")
    # absolute overlapping path
    path = test_storage.make_full_path_safe(os.path.abspath(root_path))
    assert path.endswith(root_path)
    assert test_storage.make_full_path_safe("") == test_storage.storage_path
    assert test_storage.make_full_path_safe(".") == test_storage.storage_path


def test_in_storage(test_storage: FileStorage) -> None:
    # always relative to storage root
    assert test_storage.is_path_in_storage("a/b/c") is True
    assert test_storage.is_path_in_storage(f"../{TEST_STORAGE_ROOT}/b/c") is True
    assert test_storage.is_path_in_storage("../a/b/c") is False
    assert test_storage.is_path_in_storage("../../../a/b/c") is False
    assert test_storage.is_path_in_storage("/a") is False
    assert test_storage.is_path_in_storage(".") is True
    assert test_storage.is_path_in_storage(os.curdir) is True
    assert test_storage.is_path_in_storage(os.path.realpath(os.curdir)) is False
    assert (
        test_storage.is_path_in_storage(
            os.path.join(os.path.realpath(os.curdir), TEST_STORAGE_ROOT)
        )
        is True
    )


def test_from_wd_to_relative_path(test_storage: FileStorage) -> None:
    with pytest.raises(ValueError):
        test_storage.from_wd_to_relative_path(".")
    with pytest.raises(ValueError):
        test_storage.from_wd_to_relative_path("")
    with pytest.raises(ValueError):
        test_storage.from_wd_to_relative_path("chess.py")

    with set_working_dir(TEST_STORAGE_ROOT):
        assert test_storage.from_wd_to_relative_path(".") == "."
        assert test_storage.from_wd_to_relative_path("") == "."
        assert test_storage.from_wd_to_relative_path("a/b/c") == str(Path("a/b/c"))

    test_storage.create_folder("a")
    with set_working_dir(os.path.join(TEST_STORAGE_ROOT, "a")):
        assert test_storage.from_wd_to_relative_path(".") == "a"
        assert test_storage.from_wd_to_relative_path("") == "a"
        assert test_storage.from_wd_to_relative_path("a/b/c") == str(Path("a/a/b/c"))


def test_hard_links(test_storage: FileStorage) -> None:
    content = uniq_id()
    test_storage.save("file.txt", content)
    test_storage.link_hard("file.txt", "link.txt")
    # it is a file
    assert test_storage.has_file("link.txt")
    # should have same content as file
    assert test_storage.load("link.txt") == content
    # should be linked
    with test_storage.open_file("file.txt", mode="a") as f:
        f.write(content)
    assert test_storage.load("link.txt") == content * 2
    with test_storage.open_file("link.txt", mode="a") as f:
        f.write(content)
    assert test_storage.load("file.txt") == content * 3
    # delete original file
    test_storage.delete("file.txt")
    assert not test_storage.has_file("file.txt")
    assert test_storage.load("link.txt") == content * 3


def test_validate_file_name_component() -> None:
    # no dots
    with pytest.raises(ValueError):
        FileStorage.validate_file_name_component("a.b")
    # no slashes
    with pytest.raises(ValueError):
        FileStorage.validate_file_name_component("a/b")
    # no backslashes
    with pytest.raises(ValueError):
        FileStorage.validate_file_name_component("a\\b")

    FileStorage.validate_file_name_component("BAN__ANA is allowed")


@pytest.mark.parametrize("action", ("rename_tree_files", "rename_tree", "atomic_rename"))
def test_rename_nested_tree(test_storage: FileStorage, action: str) -> None:
    source_dir = os.path.join(test_storage.storage_path, "source")
    nested_dir_1 = os.path.join(source_dir, "nested1")
    nested_dir_2 = os.path.join(nested_dir_1, "nested2")
    empty_dir = os.path.join(source_dir, "empty")
    os.makedirs(nested_dir_2)
    os.makedirs(empty_dir)
    with open(os.path.join(source_dir, "test1.txt"), "w", encoding="utf-8") as f:
        f.write("test")
    with open(os.path.join(nested_dir_1, "test2.txt"), "w", encoding="utf-8") as f:
        f.write("test")
    with open(os.path.join(nested_dir_2, "test3.txt"), "w", encoding="utf-8") as f:
        f.write("test")

    dest_dir = os.path.join(test_storage.storage_path, "dest")

    getattr(test_storage, action)(source_dir, dest_dir)

    assert not os.path.exists(source_dir)
    assert os.path.exists(dest_dir)
    assert os.path.exists(os.path.join(dest_dir, "nested1"))
    assert os.path.exists(os.path.join(dest_dir, "nested1", "nested2"))
    assert os.path.exists(os.path.join(dest_dir, "empty"))
    assert os.path.exists(os.path.join(dest_dir, "test1.txt"))
    assert os.path.exists(os.path.join(dest_dir, "nested1", "test2.txt"))
    assert os.path.exists(os.path.join(dest_dir, "nested1", "nested2", "test3.txt"))


@skipifnotwindows
def test_rmtree_ro(test_storage: FileStorage) -> None:
    test_storage.create_folder("protected")
    path = test_storage.save("protected/barbapapa.txt", "barbapapa")
    os.chmod(path, stat.S_IREAD)
    os.chmod(test_storage.make_full_path_safe("protected"), stat.S_IREAD)
    with pytest.raises(PermissionError):
        test_storage.delete_folder("protected", recursively=True, delete_ro=False)
    test_storage.delete_folder("protected", recursively=True, delete_ro=True)
    assert not test_storage.has_folder("protected")


def test_encoding_for_mode() -> None:
    assert encoding_for_mode("b") is None
    assert encoding_for_mode("bw") is None
    assert encoding_for_mode("t") == "utf-8"
    assert encoding_for_mode("a") == "utf-8"
    assert encoding_for_mode("w") == "utf-8"


def test_save_atomic_encode() -> None:
    tstr = "data'ऄअआइ''ईउऊऋऌऍऎए');"
    FileStorage.save_atomic(TEST_STORAGE_ROOT, "file.txt", tstr)
    storage = FileStorage(TEST_STORAGE_ROOT)
    with storage.open_file("file.txt") as f:
        assert cast(TextIO, f).encoding == "utf-8"
        assert f.read() == tstr

    bstr = b"axa\0x0\0x0"
    FileStorage.save_atomic(TEST_STORAGE_ROOT, "file.bin", bstr, file_type="b")
    storage = FileStorage(TEST_STORAGE_ROOT, file_type="b")
    with storage.open_file("file.bin", mode="r") as f:
        assert hasattr(f, "encoding") is False
        assert f.read() == bstr


def test_open_compressed() -> None:
    tstr = "dataisfunindeed"
    storage = FileStorage(TEST_STORAGE_ROOT)
    fname = storage.make_full_path("file.txt.gz")
    with gzip.open(fname, "wb") as f:
        f.write(tstr.encode("utf-8"))
    with open(fname[:-3], "wb") as f:
        f.write(tstr.encode("utf-8"))

    # Ensure open_file() can open compressed files
    with storage.open_file("file.txt.gz", mode="r") as f:
        assert f.read() == tstr

    # Ensure open_file() can open uncompressed files
    with storage.open_file("file.txt", mode="r") as f:
        assert f.read() == tstr

    bstr = b"axa\0x0\0x0"
    with gzip.open(fname, "wb") as f:
        f.write(bstr)
    with open(fname[:-3], "wb") as f:
        f.write(bstr)

    # Ensure open_file() can open compressed files in binary mode
    with storage.open_file("file.txt.gz", mode="rb") as f:
        assert f.read() == bstr

    # Ensure open_file() can open uncompressed files in binary mode
    with storage.open_file("file.txt", mode="rb") as f:
        assert f.read() == bstr

    # Ensure open_file() can open compressed files in text mode
    with storage.open_file("file.txt.gz", mode="rt") as f:
        assert f.read() == bstr.decode("utf-8")

    # Ensure open_file() can open uncompressed files in text mode
    with storage.open_file("file.txt", mode="rt") as f:
        assert f.read() == bstr.decode("utf-8")

    # `r` defaults to `rt` for gzip to mimic the behavior of `open()`
    with storage.open_file("file.txt.gz", mode="r") as f:
        content = f.read()
        assert isinstance(content, str)
        assert content == bstr.decode("utf-8")


def test_hard_link() -> None:
    storage = FileStorage(TEST_STORAGE_ROOT, file_type="b")
    storage.save("file.b", b"data")
    FileStorage.link_hard_with_fallback(
        storage.make_full_path("file.b"), storage.make_full_path("file.b.2")
    )
    assert storage.load("file.b.2") == b"data"
    storage.delete("file.b")
    assert storage.load("file.b.2") == b"data"
    storage.delete("file.b.2")


def test_hard_link_fallback() -> None:
    if not os.path.exists("/run/lock"):
        pytest.skip("/run/lock not found - skipping link fallback")
    with open("/run/lock/dlt.r", "wb") as f:
        f.write(b"data")
    storage = FileStorage(TEST_STORAGE_ROOT, file_type="b")
    with pytest.raises(OSError):
        os.link("/run/lock/dlt.r", storage.make_full_path("file.b.2"))
    FileStorage.link_hard_with_fallback("/run/lock/dlt.r", storage.make_full_path("file.b.2"))
    assert storage.load("file.b.2") == b"data"
    os.unlink("/run/lock/dlt.r")
    assert storage.load("file.b.2") == b"data"
    storage.delete("file.b.2")
