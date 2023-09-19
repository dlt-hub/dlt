---
sidebar_label: transactional_file
title: common.storages.transactional_file
---

Transactional file system operations.

The lock implementation allows for multiple readers and a single writer.
It can be used to operate on a single file atomically both locally and on
cloud storage. The lock can be used to operate on entire directories by
creating a lock file that resolves to an agreed upon path across processes.

#### lock\_id

```python
def lock_id(k: int = 4) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/storages/transactional_file.py#L22)

Generate a time based random id.

**Arguments**:

- `k` - The length of the suffix after the timestamp.
  

**Returns**:

  A time sortable uuid.

## Heartbeat Objects

```python
class Heartbeat(Timer)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/storages/transactional_file.py#L35)

A thread designed to periodically execute a fn.

## TransactionalFile Objects

```python
class TransactionalFile()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/storages/transactional_file.py#L45)

A transaction handler which wraps a file path.

#### \_\_init\_\_

```python
def __init__(path: str, fs: fsspec.AbstractFileSystem) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/storages/transactional_file.py#L51)

Creates a new FileTransactionHandler.

**Arguments**:

- `path` - The path to lock.
- `fs` - The fsspec file system.

#### read

```python
def read() -> t.Optional[bytes]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/storages/transactional_file.py#L116)

Reads data from the file if it exists.

#### write

```python
def write(content: bytes) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/storages/transactional_file.py#L122)

Writes data within the transaction.

#### rollback

```python
def rollback() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/storages/transactional_file.py#L130)

Rolls back the transaction.

#### acquire\_lock

```python
def acquire_lock(blocking: bool = True,
                 timeout: float = -1,
                 jitter_mean: float = 0) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/storages/transactional_file.py#L139)

Acquires a lock on a path. Mimics the stdlib's `threading.Lock` interface.

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

#### release\_lock

```python
def release_lock() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/storages/transactional_file.py#L192)

Releases a lock on a key.

This is idempotent and safe to call multiple times.

#### lock

```python
@contextmanager
def lock(timeout: t.Optional[float] = None) -> t.Iterator[None]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/storages/transactional_file.py#L204)

A context manager that acquires and releases a lock on a path.

This is a convenience method for acquiring a lock and reading the contents of
the file. It will release the lock when the context manager exits. This is
useful for reading a file and then writing it back out as a transaction. If
the lock cannot be acquired, this will raise a RuntimeError. If the lock is
acquired, the contents of the file will be returned. If the file does not
exist, None will be returned. If an exception is raised within the context
manager, the transaction will be rolled back.

**Arguments**:

- `timeout` - The timeout for acquiring the lock. If None, this will use the
  default timeout. A timeout of -1 will wait indefinitely.
  

**Raises**:

- `RuntimeError` - If the lock cannot be acquired.

#### \_\_del\_\_

```python
def __del__() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/storages/transactional_file.py#L234)

Stop the heartbeat thread on gc. Locks should be released explicitly.

