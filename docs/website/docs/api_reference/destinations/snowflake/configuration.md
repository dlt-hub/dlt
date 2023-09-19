---
sidebar_label: configuration
title: destinations.snowflake.configuration
---

## SnowflakeCredentials Objects

```python
@configspec
class SnowflakeCredentials(ConnectionStringCredentials)
```

#### drivername

type: ignore[misc]

## SnowflakeClientConfiguration Objects

```python
@configspec
class SnowflakeClientConfiguration(DestinationClientDwhWithStagingConfiguration
                                   )
```

#### destination\_name

type: ignore[misc]

#### stage\_name

Use an existing named stage instead of the default. Default uses the implicit table stage per table

#### keep\_staged\_files

Whether to keep or delete the staged files after COPY INTO succeeds

#### fingerprint

```python
def fingerprint() -> str
```

Returns a fingerprint of host part of a connection string

