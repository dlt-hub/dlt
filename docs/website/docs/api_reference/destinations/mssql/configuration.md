---
sidebar_label: configuration
title: destinations.mssql.configuration
---

## MsSqlCredentials Objects

```python
@configspec
class MsSqlCredentials(ConnectionStringCredentials)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/mssql/configuration.py#L14)

#### drivername

type: ignore

## MsSqlClientConfiguration Objects

```python
@configspec
class MsSqlClientConfiguration(DestinationClientDwhWithStagingConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/mssql/configuration.py#L76)

#### destination\_name

type: ignore

#### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/mssql/configuration.py#L82)

Returns a fingerprint of host part of a connection string

