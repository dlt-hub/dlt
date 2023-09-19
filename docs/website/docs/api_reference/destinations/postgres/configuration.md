---
sidebar_label: configuration
title: destinations.postgres.configuration
---

## PostgresCredentials Objects

```python
@configspec
class PostgresCredentials(ConnectionStringCredentials)
```

#### drivername

type: ignore

## PostgresClientConfiguration Objects

```python
@configspec
class PostgresClientConfiguration(DestinationClientDwhWithStagingConfiguration
                                  )
```

#### destination\_name

type: ignore

#### fingerprint

```python
def fingerprint() -> str
```

Returns a fingerprint of host part of a connection string

