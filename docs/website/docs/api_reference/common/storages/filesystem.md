---
sidebar_label: filesystem
title: common.storages.filesystem
---

## FileItem Objects

```python
class FileItem(TypedDict)
```

A DataItem representing a file

#### filesystem

```python
def filesystem(
    protocol: str,
    credentials: FileSystemCredentials = None
) -> Tuple[AbstractFileSystem, str]
```

Instantiates an authenticated fsspec `FileSystem` for a given `protocol` and credentials.

Please supply credentials instance corresponding to the protocol

#### filesystem\_from\_config

```python
def filesystem_from_config(
        config: FilesystemConfiguration) -> Tuple[AbstractFileSystem, str]
```

Instantiates an authenticated fsspec `FileSystem` from `config` argument.

Authenticates following filesystems:
* s3
* az, abfs
* gcs, gs

All other filesystems are not authenticated

Returns: (fsspec filesystem, normalized url)

