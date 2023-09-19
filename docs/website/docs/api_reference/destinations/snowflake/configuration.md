---
sidebar_label: configuration
title: destinations.snowflake.configuration
---

## SnowflakeCredentials Objects

```python
@configspec
class SnowflakeCredentials(ConnectionStringCredentials)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/snowflake/configuration.py#L37)

#### drivername

type: ignore[misc]

## SnowflakeClientConfiguration Objects

```python
@configspec
class SnowflakeClientConfiguration(DestinationClientDwhWithStagingConfiguration
                                   )
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/snowflake/configuration.py#L87)

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

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/snowflake/configuration.py#L96)

Returns a fingerprint of host part of a connection string

