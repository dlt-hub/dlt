---
sidebar_label: configuration
title: destinations.impl.snowflake.configuration
---

## SnowflakeCredentials Objects

```python
@configspec(init=False)
class SnowflakeCredentials(ConnectionStringCredentials)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/snowflake/configuration.py#L56)

### drivername

type: ignore[misc]

## SnowflakeClientConfiguration Objects

```python
@configspec
class SnowflakeClientConfiguration(DestinationClientDwhWithStagingConfiguration
                                   )
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/snowflake/configuration.py#L126)

### destination\_type

type: ignore[misc]

### stage\_name

Use an existing named stage instead of the default. Default uses the implicit table stage per table

### keep\_staged\_files

Whether to keep or delete the staged files after COPY INTO succeeds

### csv\_format

Optional csv format configuration

### query\_tag

A tag with placeholders to tag sessions executing jobs

### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/snowflake/configuration.py#L141)

Returns a fingerprint of host part of a connection string

