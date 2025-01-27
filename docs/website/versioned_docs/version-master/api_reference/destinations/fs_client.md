---
sidebar_label: fs_client
title: destinations.fs_client
---

## FSClientBase Objects

```python
class FSClientBase(ABC)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/fs_client.py#L10)

### get\_table\_dir

```python
@abstractmethod
def get_table_dir(table_name: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/fs_client.py#L20)

returns directory for given table

### get\_table\_dirs

```python
@abstractmethod
def get_table_dirs(table_names: Iterable[str]) -> List[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/fs_client.py#L25)

returns directories for given table

### list\_table\_files

```python
@abstractmethod
def list_table_files(table_name: str) -> List[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/fs_client.py#L30)

returns all filepaths for a given table

### truncate\_tables

```python
@abstractmethod
def truncate_tables(table_names: List[str]) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/fs_client.py#L35)

truncates the given table

### read\_bytes

```python
def read_bytes(path: str,
               start: Any = None,
               end: Any = None,
               **kwargs: Any) -> bytes
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/fs_client.py#L39)

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

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/fs_client.py#L43)

reads given file into string, tries gzip and pure text

