---
sidebar_label: configuration
title: destinations.duckdb.configuration
---

## DuckDbBaseCredentials Objects

```python
@configspec
class DuckDbBaseCredentials(ConnectionStringCredentials)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/duckdb/configuration.py#L19)

#### read\_only

open database read/write

## DuckDbCredentials Objects

```python
@configspec
class DuckDbCredentials(DuckDbBaseCredentials)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/duckdb/configuration.py#L92)

#### drivername

type: ignore

## DuckDbClientConfiguration Objects

```python
@configspec
class DuckDbClientConfiguration(DestinationClientDwhWithStagingConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/duckdb/configuration.py#L178)

#### destination\_name

type: ignore

#### create\_indexes

should unique indexes be created, this slows loading down massively

