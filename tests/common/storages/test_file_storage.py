import os
import stat
import pytest

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
    assert test_storage.to_relative_path("a/b/c") == "a/b/c"
    assert test_storage.to_relative_path("a/b/../..") == "."
    with pytest.raises(ValueError):
        test_storage.to_relative_path("a/b/../.././..")
    with pytest.raises(ValueError):
        test_storage.to_relative_path("../a/b/c")
    abs_path = os.path.join(test_storage.storage_path, "a/b/c")
    assert test_storage.to_relative_path(abs_path) == "a/b/c"
    abs_path = os.path.join(test_storage.storage_path, "a/b/../c")
    assert test_storage.to_relative_path(abs_path) == "a/c"


def test_make_full_path(test_storage: FileStorage) -> None:
    # fully within storage
    relative_path = os.path.join("dir", "to", "file")
    path = test_storage.make_full_path(relative_path)
    assert path.endswith(os.path.join(TEST_STORAGE_ROOT, relative_path))
    # overlapped with storage
    root_path = os.path.join(TEST_STORAGE_ROOT, relative_path)
    path = test_storage.make_full_path(root_path)
    assert path.endswith(root_path)
    assert path.count(TEST_STORAGE_ROOT) == 2
    # absolute path with different root than TEST_STORAGE_ROOT does not lead into storage so calculating full path impossible
    with pytest.raises(ValueError):
        test_storage.make_full_path(os.path.join("/", root_path))
    # relative path out of the root
    with pytest.raises(ValueError):
        test_storage.make_full_path("..")
    # absolute overlapping path
    path = test_storage.make_full_path(os.path.abspath(root_path))
    assert path.endswith(root_path)
    assert test_storage.make_full_path("") == test_storage.storage_path
    assert test_storage.make_full_path(".") == test_storage.storage_path


def test_in_storage(test_storage: FileStorage) -> None:
    # always relative to storage root
    assert test_storage.in_storage("a/b/c") is True
    assert test_storage.in_storage(f"../{TEST_STORAGE_ROOT}/b/c") is True
    assert test_storage.in_storage("../a/b/c") is False
    assert test_storage.in_storage("../../../a/b/c") is False
    assert test_storage.in_storage("/a") is False
    assert test_storage.in_storage(".") is True
    assert test_storage.in_storage(os.curdir) is True
    assert test_storage.in_storage(os.path.realpath(os.curdir)) is False
    assert test_storage.in_storage(os.path.join(os.path.realpath(os.curdir), TEST_STORAGE_ROOT)) is True


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
        assert test_storage.from_wd_to_relative_path("a/b/c") == "a/b/c"

    test_storage.create_folder("a")
    with set_working_dir(os.path.join(TEST_STORAGE_ROOT, "a")):
        assert test_storage.from_wd_to_relative_path(".") == "a"
        assert test_storage.from_wd_to_relative_path("") == "a"
        assert test_storage.from_wd_to_relative_path("a/b/c") == "a/a/b/c"


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


@skipifnotwindows
def test_rmtree_ro(test_storage: FileStorage) -> None:
    # testing only on windows because
    # rmtree deletes read only files on POSIXes anyway
    # the write protected dir protects also files we we would need to remove the protection before trying again
    test_storage.create_folder("protected")
    path = test_storage.save("protected/barbapapa.txt", "barbapapa")
    os.chmod(path, stat.S_IREAD)
    os.chmod(test_storage.make_full_path("protected"), stat.S_IREAD)
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
        assert f.encoding == "utf-8"
        assert f.read() == tstr

    bstr = b"axa\0x0\0x0"
    FileStorage.save_atomic(TEST_STORAGE_ROOT, "file.bin", bstr, file_type="b")
    storage = FileStorage(TEST_STORAGE_ROOT, file_type="b")
    with storage.open_file("file.bin", mode="r") as f:
        assert hasattr(f, "encoding") is False
        assert f.read() == bstr

