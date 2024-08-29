---
sidebar_label: configuration
title: destinations.impl.mssql.configuration
---

## MsSqlCredentials Objects

```python
@configspec
class MsSqlCredentials(ConnectionStringCredentials)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/mssql/configuration.py#L14)

### drivername

type: ignore

## MsSqlClientConfiguration Objects

```python
@configspec
class MsSqlClientConfiguration(DestinationClientDwhWithStagingConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/mssql/configuration.py#L92)

### destination\_type

type: ignore

### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/mssql/configuration.py#L98)

Returns a fingerprint of host part of a connection string

