---
sidebar_label: configuration
title: destinations.impl.postgres.configuration
---

## PostgresCredentials Objects

```python
@configspec
class PostgresCredentials(ConnectionStringCredentials)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/postgres/configuration.py#L13)

### drivername

type: ignore

## PostgresClientConfiguration Objects

```python
@configspec
class PostgresClientConfiguration(DestinationClientDwhWithStagingConfiguration
                                  )
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/postgres/configuration.py#L35)

### destination\_type

type: ignore

### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/postgres/configuration.py#L41)

Returns a fingerprint of host part of a connection string

