---
sidebar_label: configuration
title: destinations.impl.postgres.configuration
---

## PostgresCredentials Objects

```python
@configspec(init=False)
class PostgresCredentials(ConnectionStringCredentials)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/postgres/configuration.py#L14)

### drivername

type: ignore

## PostgresClientConfiguration Objects

```python
@configspec
class PostgresClientConfiguration(DestinationClientDwhWithStagingConfiguration
                                  )
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/postgres/configuration.py#L36)

### destination\_type

type: ignore

### csv\_format

Optional csv format configuration

### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/postgres/configuration.py#L45)

Returns a fingerprint of host part of a connection string

