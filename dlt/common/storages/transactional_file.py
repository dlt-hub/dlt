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
from pathlib import Path
import posixpath
from contextlib import contextmanager
from threading import Timer
import fsspec

from dlt.common.pendulum import pendulum, timedelta
from dlt.common.storages.fsspec_filesystem import MTIME_DISPATCH


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
    daemon = True

    def run(self) -> None:
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)
        self.finished.set()


class TransactionalFile:
    """A transaction handler which wraps a file path."""

    POLLING_INTERVAL = 0.5
    LOCK_TTL_SECONDS = 30.0

    def __init__(self, path: str, fs: fsspec.AbstractFileSystem) -> None:
        """Creates a new FileTransactionHandler.

        Args:
            path: The path to lock.
            fs: The fsspec file system.
        """
        proto = fs.protocol[0] if isinstance(fs.protocol, (list, tuple)) else fs.protocol
        self.extract_mtime = MTIME_DISPATCH.get(proto, MTIME_DISPATCH["file"])

        parsed_path = Path(path)
        if not parsed_path.is_absolute():
            raise ValueError(f"{path} is not absolute. Please pass only absolute paths to TransactionalFile")
        self.path = path
        if proto == "file":
            # standardize path separator to POSIX. fsspec always uses POSIX. Windows may use either.
            self.path = parsed_path.as_posix()

        self.lock_prefix = f"{self.path}.lock"

        self._fs = fs
        self._original_contents: t.Optional[bytes] = None
        self._is_locked = False
        self._heartbeat: t.Optional[Heartbeat] = None

    def _start_heartbeat(self) -> Heartbeat:
        """Create a thread that will periodically update the mtime."""
        self._stop_heartbeat()
        self._heartbeat = Heartbeat(
            TransactionalFile.LOCK_TTL_SECONDS / 2,
            self._fs.touch,
            args=(self.lock_path,),
        )
        self._heartbeat.start()
        return self._heartbeat

    def _stop_heartbeat(self) -> None:
        """Stop the heartbeat thread if it exists."""
        if self._heartbeat is not None:
            self._heartbeat.cancel()
            self._heartbeat = None

    def _sync_locks(self) -> t.List[str]:
        """Gets a list of lock names after removing stale locks. The list is time-sortable with earliest created lock coming first."""
        output = []
        now = pendulum.now()

        for lock in self._fs.ls(posixpath.dirname(self.lock_path), refresh=True, detail=True):
            name = lock["name"]
            if not name.startswith(self.lock_prefix):
                continue
            # Purge stale locks
            mtime = self.extract_mtime(lock)
            if now - mtime > timedelta(seconds=TransactionalFile.LOCK_TTL_SECONDS):
                try: # Janitors can race, so we ignore errors
                    self._fs.rm(name)
                except OSError:
                    pass
                continue
            # The name is timestamp + random suffix and is time sortable
            output.append(name)
        if not output:
            raise RuntimeError(f"When syncing locks for path {self.path} and lock {self.lock_path} no lock file was found")
        return output

    def read(self) -> t.Optional[bytes]:
        """Reads data from the file if it exists."""
        if self._fs.isfile(self.path):
            return t.cast(bytes, self._fs.cat(self.path))
        return None

    def write(self, content: bytes) -> None:
        """Writes data within the transaction."""
        if not self._is_locked:
            raise RuntimeError("Cannot write to a file without a lock.")
        if self._fs.isdir(self.path):
            raise RuntimeError("Cannot write to a directory.")
        self._fs.pipe(self.path, content)

    def rollback(self) -> None:
        """Rolls back the transaction."""
        if not self._is_locked:
            raise RuntimeError("Cannot rollback without a lock.")
        if self._original_contents is not None:
            self.write(self._original_contents)
        elif self._fs.isfile(self.path):
            self._fs.rm(self.path)

    def acquire_lock(self, blocking: bool = True, timeout: float = -1, jitter_mean: float = 0) -> bool:
        """Acquires a lock on a path. Mimics the stdlib's `threading.Lock` interface.

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

        If you expect a large concurrency on the lock, you can set the jitter_mean to a
        value > 0. This will introduce a short random gap before locking procedure
        starts.

        The return value is True if the lock is acquired successfully, False if
        not (for example if the timeout expired).
        """
        if self._is_locked:
            return True

        if jitter_mean > 0:
            time.sleep(random.random() * jitter_mean)  # Add jitter to avoid thundering herd
        self.lock_path = f"{self.lock_prefix}.{lock_id()}"
        self._fs.touch(self.lock_path)
        locks = self._sync_locks()
        active_lock = min(locks)
        start = time.time()

        while active_lock != self.lock_path:
            if not blocking or (timeout > 0 and time.time() - start > timeout):
                self._fs.rm(self.lock_path)
                return False

            time.sleep(random.random() + TransactionalFile.POLLING_INTERVAL)
            locks = self._sync_locks()
            if self.lock_path not in locks:
                self._fs.touch(self.lock_path)
                locks = self._sync_locks()

            active_lock = min(locks)

        self._original_contents = self.read()
        self._is_locked = True
        self._start_heartbeat()
        return True

    def release_lock(self) -> None:
        """Releases a lock on a key.

        This is idempotent and safe to call multiple times.
        """
        if self._is_locked:
            self._stop_heartbeat()
            self._fs.rm(self.lock_path)
            self._is_locked = False
            self._original_contents = None

    @contextmanager
    def lock(self, timeout: t.Optional[float] = None) -> t.Iterator[None]:
        """A context manager that acquires and releases a lock on a path.

        This is a convenience method for acquiring a lock and reading the contents of
        the file. It will release the lock when the context manager exits. This is
        useful for reading a file and then writing it back out as a transaction. If
        the lock cannot be acquired, this will raise a RuntimeError. If the lock is
        acquired, the contents of the file will be returned. If the file does not
        exist, None will be returned. If an exception is raised within the context
        manager, the transaction will be rolled back.

        Args:
            timeout: The timeout for acquiring the lock. If None, this will use the
                default timeout. A timeout of -1 will wait indefinitely.

        Raises:
            RuntimeError: If the lock cannot be acquired.
        """
        if timeout is None:
            timeout = TransactionalFile.LOCK_TTL_SECONDS + 1
        if not self.acquire_lock(timeout=timeout):
            raise RuntimeError("Could not acquire lock.")
        try:
            yield
        except Exception:
            self.rollback()
            raise
        finally:
            self.release_lock()

    def __del__(self) -> None:
        """Stop the heartbeat thread on gc. Locks should be released explicitly."""
        self._stop_heartbeat()
