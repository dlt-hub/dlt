---
sidebar_label: filesystem
title: common.storages.filesystem
---

## FileItem Objects

```python
class FileItem(TypedDict)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/storages/filesystem.py#L16)

A DataItem representing a file

#### filesystem

```python
def filesystem(
    protocol: str,
    credentials: FileSystemCredentials = None
) -> Tuple[AbstractFileSystem, str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/storages/filesystem.py#L41)

Instantiates an authenticated fsspec `FileSystem` for a given `protocol` and credentials.

Please supply credentials instance corresponding to the protocol

#### filesystem\_from\_config

```python
def filesystem_from_config(
        config: FilesystemConfiguration) -> Tuple[AbstractFileSystem, str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/storages/filesystem.py#L50)

Instantiates an authenticated fsspec `FileSystem` from `config` argument.

Authenticates following filesystems:
* s3
* az, abfs
* gcs, gs

All other filesystems are not authenticated

Returns: (fsspec filesystem, normalized url)

