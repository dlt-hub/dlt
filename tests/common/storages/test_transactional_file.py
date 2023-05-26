import time
from tempfile import NamedTemporaryFile
from threading import Thread

import fsspec
import pytest

from dlt.common.storages.transactions import TransactionalFile


@pytest.fixture(scope="session")
def fs() -> fsspec.AbstractFileSystem:
    return fsspec.filesystem("file")


def test_file_transaction_with_content(fs: fsspec.AbstractFileSystem):
    with NamedTemporaryFile() as f:
        # Seed the file with some content
        f.write(b"test 1")
        f.flush()
        file = TransactionalFile(f.name, fs)

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


def test_file_transaction_no_content(fs: fsspec.AbstractFileSystem):
    with NamedTemporaryFile() as f:
        file = TransactionalFile(f.name, fs)
        with pytest.raises(RuntimeError):
            file.write(b"test 1")
        file.acquire_lock()
        file.write(b"test 1")
        assert file.read() == b"test 1"

        # Ensure that we can rollback to an empty file
        # if the file was empty to begin with
        assert file._original_contents == b""
        file.rollback()
        assert file.read() == b""
        file.release_lock()


def test_file_transaction_multiple_writers(fs: fsspec.AbstractFileSystem):
    with NamedTemporaryFile() as f:
        writer_1 = TransactionalFile(f.name, fs)
        writer_2 = TransactionalFile(f.name, fs)
        writer_3 = TransactionalFile(f.name, fs)
        writer_1.acquire_lock()

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


def test_file_transaction_ttl_expiry(fs: fsspec.AbstractFileSystem, monkeypatch):
    monkeypatch.setattr(TransactionalFile, "LOCK_TTL_SECONDS", 1)
    with NamedTemporaryFile() as f:
        writer_1 = TransactionalFile(f.name, fs)
        writer_2 = TransactionalFile(f.name, fs)
        writer_1.acquire_lock()
        writer_1._stop_hearbeat()
        time.sleep(2.0)
        
        # Ensure a lock can be acquired after the TTL has expired
        assert writer_2.acquire_lock(blocking=False)
        writer_2.release_lock()


def test_file_transaction_maintain_lock(fs: fsspec.AbstractFileSystem, monkeypatch):
    monkeypatch.setattr(TransactionalFile, "LOCK_TTL_SECONDS", 1)    
    with NamedTemporaryFile() as f:
        writer_1 = TransactionalFile(f.name, fs)
        writer_2 = TransactionalFile(f.name, fs)
        writer_1.acquire_lock()
        Thread(target=writer_2.acquire_lock, daemon=True).start()
        time.sleep(2.5)
        
        # Ensure another process cannot acquire the lock or write as long as
        # the first process is maintaining the lock and heartbeat
        with pytest.raises(RuntimeError):
            writer_2.write(b"test 2")
