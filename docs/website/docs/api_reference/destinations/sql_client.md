---
sidebar_label: sql_client
title: destinations.sql_client
---

## SqlClientBase Objects

```python
class SqlClientBase(ABC, Generic[TNativeConn])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/sql_client.py#L15)

#### execute\_fragments

```python
def execute_fragments(fragments: Sequence[AnyStr], *args: Any,
                      **kwargs: Any) -> Optional[Sequence[Sequence[Any]]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/sql_client.py#L92)

Executes several SQL fragments as efficiently as possible to prevent data copying. Default implementation just joins the strings and executes them together.

#### with\_alternative\_dataset\_name

```python
@contextmanager
def with_alternative_dataset_name(
        dataset_name: str) -> Iterator["SqlClientBase[TNativeConn]"]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/sql_client.py#L112)

Sets the `dataset_name` as the default dataset during the lifetime of the context. Does not modify any search paths in the existing connection.

## DBApiCursorImpl Objects

```python
class DBApiCursorImpl(DBApiCursor)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/sql_client.py#L157)

A DBApi Cursor wrapper with dataframes reading functionality

