---
sidebar_label: configuration
title: destinations.mssql.configuration
---

## MsSqlCredentials Objects

```python
@configspec
class MsSqlCredentials(ConnectionStringCredentials)
```

#### drivername

type: ignore

## MsSqlClientConfiguration Objects

```python
@configspec
class MsSqlClientConfiguration(DestinationClientDwhWithStagingConfiguration)
```

#### destination\_name

type: ignore

#### fingerprint

```python
def fingerprint() -> str
```

Returns a fingerprint of host part of a connection string

