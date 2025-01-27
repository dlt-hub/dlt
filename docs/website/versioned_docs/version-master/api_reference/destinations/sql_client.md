---
sidebar_label: sql_client
title: destinations.sql_client
---

## TJobQueryTags Objects

```python
class TJobQueryTags(TypedDict)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/sql_client.py#L44)

Applied to sql client when a job using it starts. Using to tag queries

## SqlClientBase Objects

```python
class SqlClientBase(ABC, Generic[TNativeConn])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/sql_client.py#L54)

### database\_name

Database or catalog name, optional

### dataset\_name

Normalized dataset name

### staging\_dataset\_name

Normalized staging dataset name

### capabilities

Instance of adjusted destination capabilities

### drop\_tables

```python
def drop_tables(*tables: str) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/sql_client.py#L136)

Drops a set of tables if they exist

### execute\_fragments

```python
def execute_fragments(fragments: Sequence[AnyStr], *args: Any,
                      **kwargs: Any) -> Optional[Sequence[Sequence[Any]]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/sql_client.py#L174)

Executes several SQL fragments as efficiently as possible to prevent data copying. Default implementation just joins the strings and executes them together.

### execute\_many

```python
def execute_many(statements: Sequence[str], *args: Any,
                 **kwargs: Any) -> Optional[Sequence[Sequence[Any]]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/sql_client.py#L180)

Executes multiple SQL statements as efficiently as possible. When client supports multiple statements in a single query
they are executed together in as few database calls as possible.

### make\_qualified\_table\_name\_path

```python
def make_qualified_table_name_path(table_name: Optional[str],
                                   escape: bool = True) -> List[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/sql_client.py#L215)

Returns a list with path components leading from catalog to table_name.
Used to construct fully qualified names. `table_name` is optional.

### get\_qualified\_table\_names

```python
def get_qualified_table_names(table_name: str,
                              escape: bool = True) -> Tuple[str, str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/sql_client.py#L235)

Returns qualified names for table and corresponding staging table as tuple.

### with\_alternative\_dataset\_name

```python
@contextmanager
def with_alternative_dataset_name(
        dataset_name: str) -> Iterator["SqlClientBase[TNativeConn]"]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/sql_client.py#L248)

Sets the `dataset_name` as the default dataset during the lifetime of the context. Does not modify any search paths in the existing connection.

### with\_staging\_dataset

```python
def with_staging_dataset() -> ContextManager["SqlClientBase[TNativeConn]"]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/sql_client.py#L260)

Temporarily switch sql client to staging dataset name

### is\_staging\_dataset\_active

```python
@property
def is_staging_dataset_active() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/sql_client.py#L265)

Checks if staging dataset is currently active

### set\_query\_tags

```python
def set_query_tags(tags: TJobQueryTags) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/sql_client.py#L269)

Sets current schema (source), resource, load_id and table name when a job starts

## DBApiCursorImpl Objects

```python
class DBApiCursorImpl(DBApiCursor)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/sql_client.py#L327)

A DBApi Cursor wrapper with dataframes reading functionality

### df

```python
def df(chunk_size: int = None, **kwargs: Any) -> Optional[DataFrame]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/sql_client.py#L354)

Fetches results as data frame in full or in specified chunks.

May use native pandas/arrow reader if available. Depending on
the native implementation chunk size may vary.

### arrow

```python
def arrow(chunk_size: int = None, **kwargs: Any) -> Optional[ArrowTable]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/sql_client.py#L365)

Fetches results as data frame in full or in specified chunks.

May use native pandas/arrow reader if available. Depending on
the native implementation chunk size may vary.

### iter\_df

```python
def iter_df(chunk_size: int) -> Generator[DataFrame, None, None]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/sql_client.py#L382)

Default implementation converts arrow to df

### iter\_arrow

```python
def iter_arrow(chunk_size: int) -> Generator[ArrowTable, None, None]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/sql_client.py#L391)

Default implementation converts query result to arrow table

