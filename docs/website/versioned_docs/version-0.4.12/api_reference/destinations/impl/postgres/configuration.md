---
sidebar_label: configuration
title: destinations.impl.postgres.configuration
---

## PostgresCredentials Objects

```python
@configspec(init=False)
class PostgresCredentials(ConnectionStringCredentials)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/postgres/configuration.py#L15)

### drivername

type: ignore

## PostgresClientConfiguration Objects

```python
@configspec
class PostgresClientConfiguration(DestinationClientDwhWithStagingConfiguration
                                  )
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/postgres/configuration.py#L35)

### destination\_type

type: ignore

### csv\_format

Optional csv format configuration

### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/postgres/configuration.py#L44)

Returns a fingerprint of host part of a connection string

