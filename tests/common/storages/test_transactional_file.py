import functools
import time
from tempfile import TemporaryDirectory, mktemp
from threading import Thread
from typing import Iterator

import fsspec
import pytest

from dlt.common.storages import fsspec_filesystem
from dlt.common.storages.transactional_file import TransactionalFile

from tests.utils import skipifwindows


@pytest.fixture(scope="session")
def fs() -> fsspec.AbstractFileSystem:
    return fsspec_filesystem("file")[0]


@pytest.fixture
def file_name() -> Iterator[str]:
    file = mktemp()
    yield file
    # if os.path.isfile(file):
    #     os.remove(file)


def test_file_transaction_with_content(fs: fsspec.AbstractFileSystem, file_name: str):
    with open(file_name, "wb") as f:
        # Seed the file with some content
        f.write(b"test 1")
        f.flush()

    file = TransactionalFile(file_name, fs)

    # Test that we cannot write without a lock
    with pytest.raises(RuntimeError):
        file.write(b"test 2")

    # Test that we can write with a lock
    file.acquire_lock()
    file.write(b"test 2")
    assert file.read() == b"test 2"
    assert file._original_contents == b"test 1"

    # Test that we can rollback
    file.rollback()
    assert file.read() == b"test 1"
    assert file._original_contents == b"test 1"
    file.release_lock()


def test_file_transaction_no_content(fs: fsspec.AbstractFileSystem, file_name: str):
    # file does not exist here
    file = TransactionalFile(file_name, fs)
    assert file._original_contents is None
    with pytest.raises(RuntimeError):
        file.write(b"test 1")
    file.acquire_lock()
    file.write(b"test 1")
    assert file.read() == b"test 1"

    # Ensure that we can rollback to an empty file
    # if the file was empty to begin with
    assert file._original_contents is None
    file.rollback()
    assert file.read() is None
    file.release_lock()


def test_file_transaction_multiple_writers(
    fs: fsspec.AbstractFileSystem, file_name: str
):
    writer_1 = TransactionalFile(file_name, fs)
    writer_2 = TransactionalFile(file_name, fs)
    writer_3 = TransactionalFile(file_name, fs)
    writer_1.acquire_lock()
    # races may happen due to low timer resolutions on Windows
    time.sleep(0.01)
    # Test that we cannot acquire a lock if it is already held
    assert not writer_2.acquire_lock(blocking=False)

    # Test 1 writer and multiple readers
    writer_1.write(b"test 1")
    assert writer_1.read() == b"test 1"
    assert writer_2.read() == b"test 1"
    assert writer_3.read() == b"test 1"

    # Test handover of lock
    writer_1.release_lock()
    assert writer_2.acquire_lock()

    # Test 1 writer and multiple readers
    writer_2.write(b"test 2")
    assert writer_1.read() == b"test 2"
    assert writer_2.read() == b"test 2"
    assert writer_3.read() == b"test 2"
    writer_2.release_lock()

    with writer_1.lock():
        writer_1.write(b"test 3")
        assert writer_1.read() == b"test 3"
        assert writer_2.read() == b"test 3"

        with pytest.raises(RuntimeError):
            writer_2.write(b"foo")

    with writer_2.lock():
        writer_2.write(b"test 4")
        assert writer_2.read() == b"test 4"


def test_file_transaction_multiple_writers_with_races(
    fs: fsspec.AbstractFileSystem, file_name: str
):
    writer_1 = TransactionalFile(file_name, fs)
    time.sleep(0.5)
    writer_2 = TransactionalFile(file_name, fs)

    # writer 2 acquires lock
    assert writer_2.acquire_lock() is True
    # a small gap required on windows which has a timer resolution of ~16ms
    time.sleep(0.02)
    # Test that we cannot acquire a lock if it is already held
    assert not writer_1.acquire_lock(blocking=False)
    writer_2.release_lock()


@pytest.mark.skip(reason="This is more interesting on a remote filesystem")
def test_file_transaction_simultaneous(fs: fsspec.AbstractFileSystem):
    from concurrent.futures import ThreadPoolExecutor

    pool = ThreadPoolExecutor(max_workers=40)
    results = pool.map(
        lambda _: TransactionalFile("/bucket/test_123", fs).acquire_lock(
            blocking=False, jitter_mean=0.3
        ),
        range(200),
    )
    assert sum(results) == 1


def test_file_transaction_ttl_expiry(
    fs: fsspec.AbstractFileSystem, monkeypatch, file_name: str
):
    monkeypatch.setattr(TransactionalFile, "LOCK_TTL_SECONDS", 1)
    writer_1 = TransactionalFile(file_name, fs)
    writer_2 = TransactionalFile(file_name, fs)
    writer_1.acquire_lock()
    writer_1._stop_heartbeat()
    time.sleep(2.0)

    # Ensure a lock can be acquired after the TTL has expired
    assert writer_2.acquire_lock(blocking=False)
    writer_2.release_lock()


@skipifwindows
def test_file_transaction_maintain_lock(
    fs: fsspec.AbstractFileSystem, monkeypatch, file_name: str
):
    monkeypatch.setattr(TransactionalFile, "LOCK_TTL_SECONDS", 1)
    writer_1 = TransactionalFile(file_name, fs)
    writer_2 = TransactionalFile(file_name, fs)
    writer_1.acquire_lock()

    thread = Thread(
        target=functools.partial(writer_2.acquire_lock, timeout=5), daemon=True
    )
    try:
        thread.start()
        time.sleep(2.5)

        # Ensure another process cannot acquire the lock or write as long as
        # the first process is maintaining the lock and heartbeat
        with pytest.raises(RuntimeError):
            writer_2.write(b"test 2")
    finally:
        # wait for timeout that kills thread
        thread.join()


def test_file_transaction_directory(fs: fsspec.AbstractFileSystem):
    with TemporaryDirectory() as tmpdir:
        writer = TransactionalFile(tmpdir, fs)

        # Ensure we can lock on a directory
        writer.acquire_lock()

        # Ensure we cannot write to a directory scoped lock
        with pytest.raises(RuntimeError):
            writer.write(b"test 1")

        writer.release_lock()
