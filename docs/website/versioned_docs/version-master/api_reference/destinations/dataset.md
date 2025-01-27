---
sidebar_label: dataset
title: destinations.dataset
---

## ReadableDBAPIRelation Objects

```python
class ReadableDBAPIRelation(SupportsReadableRelation)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/dataset.py#L58)

### \_\_init\_\_

```python
def __init__(*,
             readable_dataset: "ReadableDBAPIDataset",
             provided_query: Any = None,
             table_name: str = None,
             limit: int = None,
             selected_columns: Sequence[str] = None) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/dataset.py#L59)

Create a lazy evaluated relation to for the dataset of a destination

### query

```python
@property
def query() -> Any
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/dataset.py#L102)

build the query

### compute\_columns\_schema

```python
def compute_columns_schema() -> TTableSchemaColumns
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/dataset.py#L137)

provide schema columns for the cursor, may be filtered by selected columns

### cursor

```python
@contextmanager
def cursor() -> Generator[SupportsReadableRelation, Any, Any]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/dataset.py#L159)

Gets a DBApiCursor for the current relation

## ReadableDBAPIDataset Objects

```python
class ReadableDBAPIDataset(SupportsReadableDataset)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/dataset.py#L228)

Access to dataframes and arrowtables in the destination dataset via dbapi

### ibis

```python
def ibis() -> IbisBackend
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/dataset.py#L243)

return a connected ibis backend

### \_\_getitem\_\_

```python
def __getitem__(table_name: str) -> SupportsReadableRelation
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/dataset.py#L317)

access of table via dict notation

### \_\_getattr\_\_

```python
def __getattr__(table_name: str) -> SupportsReadableRelation
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/dataset.py#L321)

access of table via property notation

