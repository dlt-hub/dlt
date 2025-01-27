---
sidebar_label: configuration
title: destinations.impl.snowflake.configuration
---

## SnowflakeCredentials Objects

```python
@configspec(init=False)
class SnowflakeCredentials(ConnectionStringCredentials)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/snowflake/configuration.py#L57)

### drivername

type: ignore[misc]

## SnowflakeClientConfiguration Objects

```python
@configspec
class SnowflakeClientConfiguration(DestinationClientDwhWithStagingConfiguration
                                   )
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/snowflake/configuration.py#L127)

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

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/snowflake/configuration.py#L142)

Returns a fingerprint of host part of a connection string

