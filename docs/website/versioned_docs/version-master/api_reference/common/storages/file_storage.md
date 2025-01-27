---
sidebar_label: file_storage
title: common.storages.file_storage
---

## FileStorage Objects

```python
class FileStorage()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/file_storage.py#L17)

### list\_folder\_files

```python
def list_folder_files(relative_path: str, to_root: bool = True) -> List[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/file_storage.py#L128)

List all files in `relative_path` folder

**Arguments**:

- `relative_path` _str_ - A path to folder, relative to storage root
- `to_root` _bool, optional_ - If True returns paths to files in relation to root, if False, returns just file names. Defaults to True.
  

**Returns**:

- `List[str]` - A list of file names with optional path as per ``to_root`` parameter

### link\_hard\_with\_fallback

```python
@staticmethod
def link_hard_with_fallback(external_file_path: str,
                            to_file_path: str) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/file_storage.py#L168)

Try to create a hardlink and fallback to copying when filesystem doesn't support links

### atomic\_rename

```python
def atomic_rename(from_relative_path: str, to_relative_path: str) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/file_storage.py#L176)

Renames a path using os.rename which is atomic on POSIX, Windows and NFS v4.

Method falls back to non-atomic method in following cases:
1. On Windows when destination file exists
2. If underlying file system does not support atomic rename
3. All buckets mapped with FUSE are not atomic

### rename\_tree

```python
def rename_tree(from_relative_path: str, to_relative_path: str) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/file_storage.py#L187)

Renames a tree using os.rename if possible making it atomic

If we get 'too many open files': in that case `rename_tree_files is used

### rename\_tree\_files

```python
def rename_tree_files(from_relative_path: str, to_relative_path: str) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/file_storage.py#L201)

Renames files in a tree recursively using os.rename.

### atomic\_import

```python
def atomic_import(external_file_path: str,
                  to_folder: str,
                  new_file_name: Optional[str] = None) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/file_storage.py#L224)

Moves a file at `external_file_path` into the `to_folder` effectively importing file into storage

**Arguments**:

- `external_file_path` - Path to file to be imported
- `to_folder` - Path to folder where file should be imported
- `new_file_name` - Optional new file name for the imported file, otherwise the original file name is used
  

**Returns**:

  Path to imported file relative to storage root

### is\_path\_in\_storage

```python
def is_path_in_storage(path: str) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/file_storage.py#L243)

Checks if a given path is below storage root, without checking for item existence

### make\_full\_path\_safe

```python
def make_full_path_safe(path: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/file_storage.py#L264)

Verifies that path is under storage root and then returns normalized absolute path

### make\_full\_path

```python
def make_full_path(path: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/file_storage.py#L271)

Joins path with storage root. Intended for path known to be relative to storage root

### open\_zipsafe\_ro

```python
@staticmethod
def open_zipsafe_ro(path: str, mode: str = "r", **kwargs: Any) -> IO[Any]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/file_storage.py#L310)

Opens a file using gzip.open if it is a gzip file, otherwise uses open.

### is\_gzipped

```python
@staticmethod
def is_gzipped(path: str) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/file_storage.py#L326)

Checks if file under path is gzipped by reading a header

