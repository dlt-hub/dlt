---
sidebar_label: fs_client
title: destinations.fs_client
---

## FSClientBase Objects

```python
class FSClientBase(ABC)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/fs_client.py#L9)

### get\_table\_dir

```python
@abstractmethod
def get_table_dir(table_name: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/fs_client.py#L19)

returns directory for given table

### get\_table\_dirs

```python
@abstractmethod
def get_table_dirs(table_names: Iterable[str]) -> List[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/fs_client.py#L24)

returns directories for given table

### list\_table\_files

```python
@abstractmethod
def list_table_files(table_name: str) -> List[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/fs_client.py#L29)

returns all filepaths for a given table

### truncate\_tables

```python
@abstractmethod
def truncate_tables(table_names: List[str]) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/fs_client.py#L34)

truncates the given table

### read\_bytes

```python
def read_bytes(path: str,
               start: Any = None,
               end: Any = None,
               **kwargs: Any) -> bytes
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/fs_client.py#L38)

reads given file to bytes object

### read\_text

```python
def read_text(path: str,
              encoding: Any = "utf-8",
              errors: Any = None,
              newline: Any = None,
              compression: str = None,
              **kwargs: Any) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/fs_client.py#L42)

reads given file into string, tries gzip and pure text

