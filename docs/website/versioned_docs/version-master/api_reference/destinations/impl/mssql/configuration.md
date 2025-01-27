---
sidebar_label: configuration
title: destinations.impl.mssql.configuration
---

## MsSqlCredentials Objects

```python
@configspec(init=False)
class MsSqlCredentials(ConnectionStringCredentials)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/mssql/configuration.py#L14)

### drivername

type: ignore

## MsSqlClientConfiguration Objects

```python
@configspec
class MsSqlClientConfiguration(DestinationClientDwhWithStagingConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/mssql/configuration.py#L92)

### destination\_type

type: ignore

### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/mssql/configuration.py#L99)

Returns a fingerprint of host part of a connection string

