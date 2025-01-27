---
sidebar_label: configuration
title: destinations.impl.mssql.configuration
---

## MsSqlCredentials Objects

```python
@configspec(init=False)
class MsSqlCredentials(ConnectionStringCredentials)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/mssql/configuration.py#L15)

### drivername

type: ignore

## MsSqlClientConfiguration Objects

```python
@configspec
class MsSqlClientConfiguration(DestinationClientDwhWithStagingConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/mssql/configuration.py#L91)

### destination\_type

type: ignore

### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/mssql/configuration.py#L98)

Returns a fingerprint of host part of a connection string

