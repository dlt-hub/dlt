---
sidebar_label: sql_client
title: destinations.sql_client
---

## SqlClientBase Objects

```python
class SqlClientBase(ABC, Generic[TNativeConn])
```

#### execute\_fragments

```python
def execute_fragments(fragments: Sequence[AnyStr], *args: Any,
                      **kwargs: Any) -> Optional[Sequence[Sequence[Any]]]
```

Executes several SQL fragments as efficiently as possible to prevent data copying. Default implementation just joins the strings and executes them together.

#### with\_alternative\_dataset\_name

```python
@contextmanager
def with_alternative_dataset_name(
        dataset_name: str) -> Iterator["SqlClientBase[TNativeConn]"]
```

Sets the `dataset_name` as the default dataset during the lifetime of the context. Does not modify any search paths in the existing connection.

## DBApiCursorImpl Objects

```python
class DBApiCursorImpl(DBApiCursor)
```

A DBApi Cursor wrapper with dataframes reading functionality

