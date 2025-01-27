---
sidebar_label: factory
title: destinations.impl.sqlalchemy.factory
---

## sqlalchemy Objects

```python
class sqlalchemy(Destination[SqlalchemyClientConfiguration,
                             "SqlalchemyJobClient"])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/sqlalchemy/factory.py#L28)

### \_\_init\_\_

```python
def __init__(credentials: t.Union[SqlalchemyCredentials, t.Dict[str, t.Any],
                                  str, "Engine"] = None,
             destination_name: t.Optional[str] = None,
             environment: t.Optional[str] = None,
             engine_args: t.Optional[t.Dict[str, t.Any]] = None,
             **kwargs: t.Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/sqlalchemy/factory.py#L94)

Configure the Sqlalchemy destination to use in a pipeline.

All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

**Arguments**:

- `credentials` - Credentials to connect to the sqlalchemy database. Can be an instance of `SqlalchemyCredentials` or
  a connection string in the format `mysql://user:password@host:port/database`
- `destination_name` - The name of the destination
- `environment` - The environment to use
- `**kwargs` - Additional arguments passed to the destination

