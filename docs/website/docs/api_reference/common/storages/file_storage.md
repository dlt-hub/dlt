---
sidebar_label: file_storage
title: common.storages.file_storage
---

## FileStorage Objects

```python
class FileStorage()
```

#### list\_folder\_files

```python
def list_folder_files(relative_path: str, to_root: bool = True) -> List[str]
```

List all files in ``relative_path`` folder

**Arguments**:

- `relative_path` _str_ - A path to folder, relative to storage root
- `to_root` _bool, optional_ - If True returns paths to files in relation to root, if False, returns just file names. Defaults to True.
  

**Returns**:

- `List[str]` - A list of file names with optional path as per ``to_root`` parameter

#### atomic\_rename

```python
def atomic_rename(from_relative_path: str, to_relative_path: str) -> None
```

Renames a path using os.rename which is atomic on POSIX, Windows and NFS v4.

Method falls back to non-atomic method in following cases:
1. On Windows when destination file exists
2. If underlying file system does not support atomic rename
3. All buckets mapped with FUSE are not atomic

#### rename\_tree

```python
def rename_tree(from_relative_path: str, to_relative_path: str) -> None
```

Renames a tree using os.rename if possible making it atomic

If we get 'too many open files': in that case `rename_tree_files is used

#### rename\_tree\_files

```python
def rename_tree_files(from_relative_path: str, to_relative_path: str) -> None
```

Renames files in a tree recursively using os.rename.

#### atomic\_import

```python
def atomic_import(external_file_path: str, to_folder: str) -> str
```

Moves a file at `external_file_path` into the `to_folder` effectively importing file into storage

#### open\_zipsafe\_ro

```python
@staticmethod
def open_zipsafe_ro(path: str, mode: str = "r", **kwargs: Any) -> IO[Any]
```

Opens a file using gzip.open if it is a gzip file, otherwise uses open.

#### is\_gzipped

```python
@staticmethod
def is_gzipped(path: str) -> bool
```

Checks if file under path is gzipped by reading a header

