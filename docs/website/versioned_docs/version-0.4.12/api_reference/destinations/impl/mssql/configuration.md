---
sidebar_label: configuration
title: destinations.impl.mssql.configuration
---

## MsSqlCredentials Objects

```python
@configspec(init=False)
class MsSqlCredentials(ConnectionStringCredentials)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/mssql/configuration.py#L15)

### drivername

type: ignore

## MsSqlClientConfiguration Objects

```python
@configspec
class MsSqlClientConfiguration(DestinationClientDwhWithStagingConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/mssql/configuration.py#L91)

### destination\_type

type: ignore

### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/mssql/configuration.py#L98)

Returns a fingerprint of host part of a connection string

