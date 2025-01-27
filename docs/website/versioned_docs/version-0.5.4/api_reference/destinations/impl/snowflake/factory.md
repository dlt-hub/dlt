---
sidebar_label: factory
title: destinations.impl.snowflake.factory
---

## snowflake Objects

```python
class snowflake(Destination[SnowflakeClientConfiguration, "SnowflakeClient"])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/snowflake/factory.py#L17)

### \_\_init\_\_

```python
def __init__(credentials: t.Union[SnowflakeCredentials, t.Dict[str, t.Any],
                                  str] = None,
             stage_name: t.Optional[str] = None,
             keep_staged_files: bool = True,
             csv_format: t.Optional[CsvFormatConfiguration] = None,
             destination_name: t.Optional[str] = None,
             environment: t.Optional[str] = None,
             **kwargs: t.Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/snowflake/factory.py#L53)

Configure the Snowflake destination to use in a pipeline.

All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

**Arguments**:

- `credentials` - Credentials to connect to the snowflake database. Can be an instance of `SnowflakeCredentials` or
  a connection string in the format `snowflake://user:password@host:port/database`
- `stage_name` - Name of an existing stage to use for loading data. Default uses implicit stage per table
- `keep_staged_files` - Whether to delete or keep staged files after loading

