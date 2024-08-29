---
sidebar_label: configuration
title: destinations.impl.duckdb.configuration
---

## DuckDbBaseCredentials Objects

```python
@configspec
class DuckDbBaseCredentials(ConnectionStringCredentials)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/duckdb/configuration.py#L22)

### read\_only

open database read/write

## DuckDbCredentials Objects

```python
@configspec
class DuckDbCredentials(DuckDbBaseCredentials)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/duckdb/configuration.py#L97)

### drivername

type: ignore

## DuckDbClientConfiguration Objects

```python
@configspec
class DuckDbClientConfiguration(DestinationClientDwhWithStagingConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/duckdb/configuration.py#L198)

### destination\_type

type: ignore

