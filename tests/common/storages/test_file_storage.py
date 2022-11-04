import os
import pytest

from dlt.common.storages.file_storage import FileStorage
from dlt.common.utils import encoding_for_mode, uniq_id

from tests.utils import TEST_STORAGE_ROOT, autouse_test_storage, test_storage


# FileStorage(TEST_STORAGE_ROOT, makedirs=True)


def test_storage_init(test_storage: FileStorage) -> None:
    # must be absolute path
    assert os.path.isabs(test_storage.storage_path)
    # may not contain file name (ends with / or \)
    assert os.path.basename(test_storage.storage_path) == ""

    # TODO: write more cases


def test_make_full_path(test_storage: FileStorage) -> None:
    # fully within storage
    relative_path = os.path.join("dir", "to", "file")
    path = test_storage.make_full_path(relative_path)
    assert path.endswith(os.path.join(TEST_STORAGE_ROOT, relative_path))
    # overlapped with storage
    root_path = os.path.join(TEST_STORAGE_ROOT, relative_path)
    path = test_storage.make_full_path(root_path)
    assert path.endswith(root_path)
    assert path.count(TEST_STORAGE_ROOT) == 1
    # absolute path with different root than TEST_STORAGE_ROOT does not lead into storage so calculating full path impossible
    with pytest.raises(ValueError):
        test_storage.make_full_path(os.path.join("/", root_path))
    # absolute overlapping path
    path = test_storage.make_full_path(os.path.abspath(root_path))
    assert path.endswith(root_path)


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

