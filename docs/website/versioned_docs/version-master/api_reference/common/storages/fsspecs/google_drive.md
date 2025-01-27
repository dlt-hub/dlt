---
sidebar_label: google_drive
title: common.storages.fsspecs.google_drive
---

## GoogleDriveFileSystem Objects

```python
class GoogleDriveFileSystem(AbstractFileSystem)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/fsspecs/google_drive.py#L50)

### \_\_init\_\_

```python
def __init__(credentials: GcpCredentials = None,
             trash_delete: bool = True,
             access: Optional[Literal["full_control",
                                      "read_only"]] = "full_control",
             spaces: Optional[Literal["drive", "appDataFolder",
                                      "photos"]] = "drive",
             **kwargs: Any)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/fsspecs/google_drive.py#L54)

Google Drive as a file-system.

The gdrive url has following format: gdrive://<root_file_id/<file_path>
Where <root_file_id> is a file id of the folder where the <file_path> is present.

Google Drive provides consistency when file ids are used. Changes are reflected immediately.
In case of listings (ls) the consistency is eventual. Changes are reflected with a delay.
As ls is used to retrieve file id from file name, we will be unable to build consistent filesystem
with google drive API. Use this with care.

Based on original fsspec Google Drive implementation: https://github.com/fsspec/gdrivefs

**Arguments**:

- `credentials` _GcpCredentials_ - Google Service credentials. If not provided, anonymous credentials
  are used
- `trash_delete` _bool_ - If True sends files to trash on rm. If False, deletes permanently.
  Note that permanent delete is not available for shared drives
  access (Optional[Literal["full_control", "read_only"]]):
  One of "full_control", "read_only".
  spaces (Optional[Literal["drive", "appDataFolder", "photos"]]):
  Category of files to search, can be 'drive', 'appDataFolder' and 'photos'.
  Of these, only the first is general.
  **kwargs:
  Passed to the parent.

### connect

```python
def connect() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/fsspecs/google_drive.py#L95)

Connect to Google Drive.

### mkdir

```python
def mkdir(path: str, create_parents: Optional[bool] = True) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/fsspecs/google_drive.py#L108)

Create a directory.

**Arguments**:

- `path` _str_ - The directory to create.
  create_parents (Optional[bool]):
  Whether to create parent directories if they don't exist.
  Defaults to True.

### makedirs

```python
def makedirs(path: str, exist_ok: Optional[bool] = True) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/fsspecs/google_drive.py#L136)

Create a directory and all its parent components.

**Arguments**:

- `path` _str_ - The directory to create.
- `exist_ok` _Optional[bool]_ - Whether to raise an error if the directory already exists.
  Defaults to True.

### rm

```python
def rm(path: str,
       recursive: Optional[bool] = True,
       maxdepth: Optional[int] = None) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/fsspecs/google_drive.py#L175)

Remove files or directories.

**Arguments**:

- `path` _str_ - The file or directory to remove.
- `recursive` _Optional[bool]_ - Whether to remove directories recursively.
  Defaults to True.
- `maxdepth` _Optional[int]_ - The maximum depth to remove directories.

### rmdir

```python
def rmdir(path: str) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/fsspecs/google_drive.py#L208)

Remove a directory.

**Arguments**:

- `path` _str_ - The directory to remove.

### export

```python
def export(path: str, mime_type: str) -> Any
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/fsspecs/google_drive.py#L230)

Convert a Google-native file to other format and download

mime_type is something like "text/plain"

### ls

```python
def ls(path: str,
       detail: Optional[bool] = False,
       refresh: Optional[bool] = False) -> Any
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/fsspecs/google_drive.py#L240)

List files in a directory.

**Arguments**:

- `path` _str_ - The directory to list.
- `detail` _Optional[bool]_ - Whether to return detailed file information.
  Defaults to False.
  

**Returns**:

- `Any` - Files in the directory data.

### path\_to\_file\_id

```python
def path_to_file_id(path: str,
                    parent_id: Optional[str] = None,
                    parent_path: str = "") -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/fsspecs/google_drive.py#L302)

Get the file ID from a path.

**Arguments**:

- `path` _str_ - The path to get the file ID from.
- `parent_id` _Optional[str]_ - The parent directory id to search.
- `parent_path` _Optional[str]_ - Path corresponding to parent id
  

**Returns**:

- `str` - The file ID.

## GoogleDriveFile Objects

```python
class GoogleDriveFile(AbstractBufferedFile)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/fsspecs/google_drive.py#L403)

### \_\_init\_\_

```python
def __init__(fs: GoogleDriveFileSystem,
             path: str,
             mode: Optional[str] = "rb",
             block_size: Optional[int] = DEFAULT_BLOCK_SIZE,
             autocommit: Optional[bool] = True,
             **kwargs: Any)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/fsspecs/google_drive.py#L404)

A Google Drive file.

**Arguments**:

- `fs` _AbstractFileSystem_ - The file system to open the file from.
- `path` _str_ - The file to open.
- `mode` _Optional[str]_ - The mode to open the file in.
  Defaults to "rb".
- `block_size` _Optional[str]_ - The block size to use.
  Defaults to DEFAULT_BLOCK_SIZE.
- `autocommit` _Optional[bool]_ - Whether to automatically commit the file.
  Defaults to True.
- `**kwargs` - Passed to the parent.

### commit

```python
def commit() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/fsspecs/google_drive.py#L513)

If not auto-committing, finalize the file.

### discard

```python
def discard() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/fsspecs/google_drive.py#L544)

Cancel in-progress multi-upload.

