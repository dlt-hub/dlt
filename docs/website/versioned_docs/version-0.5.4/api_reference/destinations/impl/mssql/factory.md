---
sidebar_label: factory
title: destinations.impl.mssql.factory
---

## mssql Objects

```python
class mssql(Destination[MsSqlClientConfiguration, "MsSqlClient"])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/mssql/factory.py#L12)

### \_\_init\_\_

```python
def __init__(credentials: t.Union[MsSqlCredentials, t.Dict[str, t.Any],
                                  str] = None,
             create_indexes: bool = True,
             destination_name: t.Optional[str] = None,
             environment: t.Optional[str] = None,
             **kwargs: t.Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/mssql/factory.py#L24)

Configure the MsSql destination to use in a pipeline.

All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

**Arguments**:

- `credentials` - Credentials to connect to the mssql database. Can be an instance of `MsSqlCredentials` or
  a connection string in the format `mssql://user:password@host:port/database`
- `create_indexes` - Should unique indexes be created
- `**kwargs` - Additional arguments passed to the destination config

