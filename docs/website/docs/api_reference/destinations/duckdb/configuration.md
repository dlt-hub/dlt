---
sidebar_label: configuration
title: destinations.duckdb.configuration
---

## DuckDbBaseCredentials Objects

```python
@configspec
class DuckDbBaseCredentials(ConnectionStringCredentials)
```

#### read\_only

open database read/write

## DuckDbCredentials Objects

```python
@configspec
class DuckDbCredentials(DuckDbBaseCredentials)
```

#### drivername

type: ignore

## DuckDbClientConfiguration Objects

```python
@configspec
class DuckDbClientConfiguration(DestinationClientDwhWithStagingConfiguration)
```

#### destination\_name

type: ignore

#### create\_indexes

should unique indexes be created, this slows loading down massively

