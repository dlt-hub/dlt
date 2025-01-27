---
sidebar_label: configuration
title: destinations.impl.duckdb.configuration
---

## DuckDbBaseCredentials Objects

```python
@configspec(init=False)
class DuckDbBaseCredentials(ConnectionStringCredentials)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/duckdb/configuration.py#L27)

### read\_only

open database read/write

## DuckDbCredentials Objects

```python
@configspec
class DuckDbCredentials(DuckDbBaseCredentials)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/duckdb/configuration.py#L108)

### drivername

type: ignore

### \_\_init\_\_

```python
def __init__(conn_or_path: Union[str, DuckDBPyConnection] = None) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/duckdb/configuration.py#L210)

Access to duckdb database at a given path or from duckdb connection

## DuckDbClientConfiguration Objects

```python
@configspec
class DuckDbClientConfiguration(DestinationClientDwhWithStagingConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/duckdb/configuration.py#L216)

### destination\_type

type: ignore

