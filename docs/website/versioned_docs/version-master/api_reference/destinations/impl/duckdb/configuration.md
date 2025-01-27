---
sidebar_label: configuration
title: destinations.impl.duckdb.configuration
---

## DuckDbBaseCredentials Objects

```python
@configspec(init=False)
class DuckDbBaseCredentials(ConnectionStringCredentials)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/duckdb/configuration.py#L29)

### read\_only

open database read/write

### has\_open\_connection

```python
@property
def has_open_connection() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/duckdb/configuration.py#L89)

Returns true if connection was not yet created or no connections were borrowed in case of external connection

## DuckDbCredentials Objects

```python
@configspec
class DuckDbCredentials(DuckDbBaseCredentials)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/duckdb/configuration.py#L109)

### drivername

type: ignore

### \_\_init\_\_

```python
def __init__(conn_or_path: Union[str, DuckDBPyConnection] = None) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/duckdb/configuration.py#L196)

Access to duckdb database at a given path or from duckdb connection

## DuckDbClientConfiguration Objects

```python
@configspec
class DuckDbClientConfiguration(DestinationClientDwhWithStagingConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/duckdb/configuration.py#L202)

### destination\_type

type: ignore

