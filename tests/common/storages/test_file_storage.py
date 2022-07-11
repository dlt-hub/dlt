from dlt.common.file_storage import FileStorage
from dlt.common.utils import encoding_for_mode

from tests.utils import TEST_STORAGE


FileStorage(TEST_STORAGE, makedirs=True)


def test_encoding_for_mode() -> None:
    assert encoding_for_mode("b") is None
    assert encoding_for_mode("bw") is None
    assert encoding_for_mode("t") == "utf-8"
    assert encoding_for_mode("a") == "utf-8"
    assert encoding_for_mode("w") == "utf-8"


def test_save_atomic_encode() -> None:
    tstr = "data'ऄअआइ''ईउऊऋऌऍऎए');"
    FileStorage.save_atomic(TEST_STORAGE, "file.txt", tstr)
    storage = FileStorage(TEST_STORAGE)
    with storage.open_file("file.txt") as f:
        assert f.encoding == "utf-8"
        assert f.read() == tstr

    bstr = b"axa\0x0\0x0"
    FileStorage.save_atomic(TEST_STORAGE, "file.bin", bstr, file_type="b")
    storage = FileStorage(TEST_STORAGE, file_type="b")
    with storage.open_file("file.bin", mode="r") as f:
        assert hasattr(f, "encoding") is False
        assert f.read() == bstr

