---
sidebar_label: fsspec_filesystem
title: common.storages.fsspec_filesystem
---

## FileItem Objects

```python
class FileItem(TypedDict)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/fsspec_filesystem.py#L39)

A DataItem representing a file

## fsspec\_filesystem

```python
def fsspec_filesystem(
    protocol: str,
    credentials: FileSystemCredentials = None,
    kwargs: Optional[DictStrAny] = None,
    client_kwargs: Optional[DictStrAny] = None
) -> Tuple[AbstractFileSystem, str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/fsspec_filesystem.py#L80)

Instantiates an authenticated fsspec `FileSystem` for a given `protocol` and credentials.

Please supply credentials instance corresponding to the protocol.
The `protocol` is just the code name of the filesystem i.e.:
* s3
* az, abfs
* gcs, gs

also see filesystem_from_config

## prepare\_fsspec\_args

```python
def prepare_fsspec_args(config: FilesystemConfiguration) -> DictStrAny
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/fsspec_filesystem.py#L101)

Prepare arguments for fsspec filesystem constructor.

**Arguments**:

- `config` _FilesystemConfiguration_ - The filesystem configuration.
  

**Returns**:

- `DictStrAny` - The arguments for the fsspec filesystem constructor.

## fsspec\_from\_config

```python
def fsspec_from_config(
        config: FilesystemConfiguration) -> Tuple[AbstractFileSystem, str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/fsspec_filesystem.py#L132)

Instantiates an authenticated fsspec `FileSystem` from `config` argument.

Authenticates following filesystems:
* s3
* az, abfs
* gcs, gs

All other filesystems are not authenticated

Returns: (fsspec filesystem, normalized url)

## FileItemDict Objects

```python
class FileItemDict(DictStrAny)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/fsspec_filesystem.py#L154)

A FileItem dictionary with additional methods to get fsspec filesystem, open and read files.

### \_\_init\_\_

```python
def __init__(mapping: FileItem,
             credentials: Optional[Union[FileSystemCredentials,
                                         AbstractFileSystem]] = None)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/fsspec_filesystem.py#L157)

Create a dictionary with the filesystem client.

**Arguments**:

- `mapping` _FileItem_ - The file item TypedDict.
- `credentials` _Optional[FileSystemCredentials], optional_ - The credentials to the
  filesystem. Defaults to None.

### fsspec

```python
@property
def fsspec() -> AbstractFileSystem
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/fsspec_filesystem.py#L173)

The filesystem client is based on the given credentials.

**Returns**:

- `AbstractFileSystem` - The fsspec client.

### open

```python
def open(mode: str = "rb",
         compression: Literal["auto", "disable", "enable"] = "auto",
         **kwargs: Any) -> IO[Any]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/fsspec_filesystem.py#L184)

Open the file as a fsspec file.

This method opens the file represented by this dictionary as a file-like object using
the fsspec library.

**Arguments**:

- `mode` _Optional[str]_ - Open mode.
- `compression` _Optional[str]_ - A flag to enable/disable compression.
  Can have one of three values: "disable" - no compression applied,
  "enable" - gzip compression applied, "auto" (default) -
  compression applied only for files compressed with gzip.
- `**kwargs` _Any_ - The arguments to pass to the fsspec open function.
  

**Returns**:

- `IOBase` - The fsspec file.

### read\_bytes

```python
def read_bytes() -> bytes
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/fsspec_filesystem.py#L242)

Read the file content.

**Returns**:

- `bytes` - The file content.

## glob\_files

```python
def glob_files(fs_client: AbstractFileSystem,
               bucket_url: str,
               file_glob: str = "**") -> Iterator[FileItem]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/fsspec_filesystem.py#L264)

Get the files from the filesystem client.

**Arguments**:

- `fs_client` _AbstractFileSystem_ - The filesystem client.
- `bucket_url` _str_ - The url to the bucket.
- `file_glob` _str_ - A glob for the filename filter.
  

**Returns**:

- `Iterable[FileItem]` - The list of files.

