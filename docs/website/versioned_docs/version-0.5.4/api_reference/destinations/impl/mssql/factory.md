---
sidebar_label: factory
title: destinations.impl.mssql.factory
---

## mssql Objects

```python
class mssql(Destination[MsSqlClientConfiguration, "MsSqlJobClient"])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/mssql/factory.py#L14)

### \_\_init\_\_

```python
def __init__(credentials: t.Union[MsSqlCredentials, t.Dict[str, t.Any],
                                  str] = None,
             create_indexes: bool = False,
             has_case_sensitive_identifiers: bool = False,
             destination_name: t.Optional[str] = None,
             environment: t.Optional[str] = None,
             **kwargs: t.Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/mssql/factory.py#L53)

Configure the MsSql destination to use in a pipeline.

All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

**Arguments**:

- `credentials` - Credentials to connect to the mssql database. Can be an instance of `MsSqlCredentials` or
  a connection string in the format `mssql://user:password@host:port/database`
- `create_indexes` - Should unique indexes be created
- `has_case_sensitive_identifiers` - Are identifiers used by mssql database case sensitive (following the collation)
- `**kwargs` - Additional arguments passed to the destination config

