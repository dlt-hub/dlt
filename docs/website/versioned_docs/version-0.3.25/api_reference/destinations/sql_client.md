---
sidebar_label: sql_client
title: destinations.sql_client
---

## SqlClientBase Objects

```python
class SqlClientBase(ABC, Generic[TNativeConn])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/sql_client.py#L32)

### execute\_fragments

```python
def execute_fragments(fragments: Sequence[AnyStr], *args: Any,
                      **kwargs: Any) -> Optional[Sequence[Sequence[Any]]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/sql_client.py#L116)

Executes several SQL fragments as efficiently as possible to prevent data copying. Default implementation just joins the strings and executes them together.

### execute\_many

```python
def execute_many(statements: Sequence[str], *args: Any,
                 **kwargs: Any) -> Optional[Sequence[Sequence[Any]]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/sql_client.py#L122)

Executes multiple SQL statements as efficiently as possible. When client supports multiple statements in a single query
they are executed together in as few database calls as possible.

### with\_alternative\_dataset\_name

```python
@contextmanager
def with_alternative_dataset_name(
        dataset_name: str) -> Iterator["SqlClientBase[TNativeConn]"]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/sql_client.py#L156)

Sets the `dataset_name` as the default dataset during the lifetime of the context. Does not modify any search paths in the existing connection.

## DBApiCursorImpl Objects

```python
class DBApiCursorImpl(DBApiCursor)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/sql_client.py#L205)

A DBApi Cursor wrapper with dataframes reading functionality

