---
sidebar_label: configuration
title: destinations.impl.snowflake.configuration
---

## SnowflakeCredentials Objects

```python
@configspec
class SnowflakeCredentials(ConnectionStringCredentials)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/snowflake/configuration.py#L55)

### drivername

type: ignore[misc]

## SnowflakeClientConfiguration Objects

```python
@configspec
class SnowflakeClientConfiguration(DestinationClientDwhWithStagingConfiguration
                                   )
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/snowflake/configuration.py#L120)

### destination\_type

type: ignore[misc]

### stage\_name

Use an existing named stage instead of the default. Default uses the implicit table stage per table

### keep\_staged\_files

Whether to keep or delete the staged files after COPY INTO succeeds

### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/snowflake/configuration.py#L129)

Returns a fingerprint of host part of a connection string

