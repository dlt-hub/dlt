"""Transactional file system operations.

The lock implementation allows for multiple readers and a single writer.
It can be used to operate on a single file atomically both locally and on
cloud storage. The lock can be used to operate on entire directories by
creating a lock file that resolves to an agreed upon path across processes.
"""
import random
import string
import time
import typing as t
from contextlib import contextmanager
from datetime import datetime, timedelta
from threading import Timer

import fsspec

POLLING_INTERVAL = 0.5
LOCK_TTL_SECONDS = 30.0


def lock_id(k: int = 4) -> str:
    """Generate a time based random id.

    Args:
        k: The length of the suffix after the timestamp.

    Returns:
        A time sortable uuid.
    """
    suffix = "".join(random.choices(string.ascii_lowercase, k=k))
    return f"{time.time_ns()}{suffix}"



class Heartbeat(Timer):
    """A thread designed to periodically execute a fn."""

    def run(self) -> None:
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)
        self.finished.set()



class TransactionalFile:
    """A transaction handler which wraps a file path."""

    # Map of protocol to mtime resolver
    # we only need to support a small finite set of protocols
    mtime_dispatch = {
        "s3": lambda f: datetime.strptime(f["LastModified"], "%Y-%m-%d %H:%M:%S%z"),
        "gcs": lambda f: datetime.strptime(f["updated"], "%Y-%m-%dT%H:%M:%S.%fZ"),
        "adl": lambda f: datetime.strptime(f["LastModified"], "%Y-%m-%d %H:%M:%S%z"),
        "file": lambda f: datetime.fromtimestamp(f["mtime"]),
    }
    # Support aliases
    mtime_dispatch["gs"] = mtime_dispatch["gcs"]
    mtime_dispatch["s3a"] = mtime_dispatch["s3"]
    mtime_dispatch["azure"] = mtime_dispatch["adl"]

    def __init__(self, path: str, fs: fsspec.AbstractFileSystem) -> None:
        """Creates a new FileTransactionHandler.

        Args:
            path: The path to lock.
            fs: The fsspec file system.
        """
        self.path = path
        self.lock_prefix = f"{self.path}.lock"
        self.lock_path = f"{self.lock_prefix}.{lock_id()}"
        proto = fs.protocol[0] if isinstance(fs.protocol, (list, tuple)) else fs.protocol
        self.get_mtime = self.mtime_dispatch.get(proto, self.mtime_dispatch["file"])
        self._fs = fs
        self._original_contents: t.Optional[bytes] = None
        self._is_locked = False
        self._heartbeat: t.Optional[Heartbeat] = None

    def _make_heartbeat_thread(self) -> Heartbeat:
        """Create a thread that will periodically update the mtime."""
        return Heartbeat(
            LOCK_TTL_SECONDS / 2,
            self._fs.touch,
            args=(self.lock_path,),
        )

    def _sync_locks(self) -> t.Dict[str, t.Tuple[float, str]]:
        """Gets a map of lock paths to their last modified times and removes stale locks."""
        output = {}
        now = datetime.now()

        for lock in self._fs.glob(self.lock_prefix + ".*", refresh=True, detail=True).values():
            mtime = self.get_mtime(lock)
            name = lock["name"]
            if now - mtime > timedelta(seconds=LOCK_TTL_SECONDS):
                # Manage stale locks
                self._fs.rm(name)
                continue
            # this creates a sortable key
            # the mtime of the file, followed by the name, which is also time sortable
            output[name] = (mtime.timestamp(), name)
        return output

    def read(self) -> t.Optional[bytes]:
        """Reads data from the file."""
        if self._fs.exists(self.path):
            return self._fs.cat(self.path)
        return None

    def write(self, content: bytes) -> None:
        """Writes data within the transaction."""
        if not self._is_locked:
            raise RuntimeError("Cannot write to a file without a lock.")
        self._fs.pipe(self.path, content)

    def rollback(self) -> None:
        """Rolls back the transaction."""
        if not self._is_locked:
            raise RuntimeError("Cannot rollback a file without a lock.")
        if self._original_contents is not None:
            self._fs.pipe(self.path, self._original_contents)

    def acquire_lock(self, blocking: bool = True, timeout: float = -1) -> bool:
        """Acquires a lock on a path.

        Acquire a lock, blocking or non-blocking.

        When invoked with the blocking argument set to True (the default), block until
        the lock is unlocked, then set it to locked and return True.

        When invoked with the blocking argument set to False, do not block. If a call
        with blocking set to True would block, return False immediately; otherwise, set
        the lock to locked and return True.

        When invoked with the floating-point timeout argument set to a positive value,
        block for at most the number of seconds specified by timeout and as long as the
        lock cannot be acquired. A timeout argument of -1 specifies an unbounded wait.
        If blocking is False, timeout is ignored. The stdlib would raise a ValueError.

        The return value is True if the lock is acquired successfully, False if
        not (for example if the timeout expired).
        """
        if self._is_locked:
            return True

        self._fs.touch(self.lock_path)
        locks = self._sync_locks()
        _, active_lock = min(locks.values())
        start = time.time()

        while active_lock != self.lock_path:
            if not blocking or (timeout > 0 and time.time() - start > timeout):
                self._fs.rm(self.lock_path)
                return False

            time.sleep(random.random() + POLLING_INTERVAL)
            locks = self._sync_locks()
            if self.lock_path not in locks:
                self._fs.touch(self.lock_path)
                locks = self._sync_locks()

            _, active_lock = min(locks.values())  # type: ignore

        self._original_contents = self.read()
        self._is_locked = True
        if self._heartbeat is not None:
            self._heartbeat.cancel()
        self._heartbeat = self._make_heartbeat_thread()
        self._heartbeat.start()
        return True

    def release_lock(self) -> None:
        """Releases a lock on a key.

        This is idempotent and safe to call multiple times.
        """
        if self._is_locked:
            self._heartbeat.cancel()
            self._heartbeat = None
            self._fs.rm(self.lock_path)
            self._is_locked = False
            self._original_contents = None

    @contextmanager
    def lock(self, timeout: float = LOCK_TTL_SECONDS + 1) -> t.Iterator[None]:
        """A context manager that acquires and releases a lock on a path.

        This is a convenience method for acquiring a lock and reading the contents of
        the file. It will release the lock when the context manager exits. This is
        useful for reading a file and then writing it back out as a transaction. If
        the lock cannot be acquired, this will raise a RuntimeError. If the lock is
        acquired, the contents of the file will be returned. If the file does not
        exist, None will be returned. If an exception is raised within the context
        manager, the transaction will be rolled back.

        Args:
            timeout: The timeout for acquiring the lock.

        Raises:
            RuntimeError: If the lock cannot be acquired.
        """
        if not self.acquire_lock(timeout=timeout):
            raise RuntimeError("Could not acquire lock.")
        try:
            yield
        except Exception:
            self.rollback()
            raise
        finally:
            self.release_lock()
