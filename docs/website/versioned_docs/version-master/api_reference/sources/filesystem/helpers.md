---
sidebar_label: helpers
title: sources.filesystem.helpers
---

Helpers for the filesystem resource.

## fsspec\_from\_resource

```python
def fsspec_from_resource(
        filesystem_instance: DltResource) -> AbstractFileSystem
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/filesystem/helpers.py#L34)

Extract authorized fsspec client from a filesystem resource

## add\_columns

```python
def add_columns(columns: List[str],
                rows: List[List[Any]]) -> List[Dict[str, Any]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/filesystem/helpers.py#L52)

Adds column names to the given rows.

**Arguments**:

- `columns` _List[str]_ - The column names.
- `rows` _List[List[Any]]_ - The rows.
  

**Returns**:

  List[Dict[str, Any]]: The rows with column names.

## fetch\_arrow

```python
def fetch_arrow(file_data, chunk_size: int) -> Iterable[TDataItem]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/filesystem/helpers.py#L69)

Fetches data from the given CSV file.

**Arguments**:

- `file_data` _DuckDBPyRelation_ - The CSV file data.
- `chunk_size` _int_ - The number of rows to read at once.
  

**Yields**:

- `Iterable[TDataItem]` - Data items, read from the given CSV file.

## fetch\_json

```python
def fetch_json(file_data, chunk_size: int) -> List[Dict[str, Any]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/filesystem/helpers.py#L83)

Fetches data from the given CSV file.

**Arguments**:

- `file_data` _DuckDBPyRelation_ - The CSV file data.
- `chunk_size` _int_ - The number of rows to read at once.
  

**Yields**:

- `Iterable[TDataItem]` - Data items, read from the given CSV file.

