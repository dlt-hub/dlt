---
sidebar_label: configuration
title: destinations.impl.postgres.configuration
---

## PostgresCredentials Objects

```python
@configspec(init=False)
class PostgresCredentials(ConnectionStringCredentials)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/postgres/configuration.py#L15)

### drivername

type: ignore

## PostgresClientConfiguration Objects

```python
@configspec
class PostgresClientConfiguration(DestinationClientDwhWithStagingConfiguration
                                  )
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/postgres/configuration.py#L35)

### destination\_type

type: ignore

### csv\_format

Optional csv format configuration

### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/postgres/configuration.py#L44)

Returns a fingerprint of host part of a connection string

