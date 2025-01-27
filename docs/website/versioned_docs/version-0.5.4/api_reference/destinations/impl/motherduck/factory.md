---
sidebar_label: factory
title: destinations.impl.motherduck.factory
---

## motherduck Objects

```python
class motherduck(Destination[MotherDuckClientConfiguration,
                             "MotherDuckClient"])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/motherduck/factory.py#L17)

### \_\_init\_\_

```python
def __init__(credentials: t.Union[MotherDuckCredentials, str, t.Dict[str,
                                                                     t.Any],
                                  "DuckDBPyConnection"] = None,
             create_indexes: bool = False,
             destination_name: t.Optional[str] = None,
             environment: t.Optional[str] = None,
             **kwargs: t.Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/motherduck/factory.py#L50)

Configure the MotherDuck destination to use in a pipeline.

All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

**Arguments**:

- `credentials` - Credentials to connect to the MotherDuck database. Can be an instance of `MotherDuckCredentials` or
  a connection string in the format `md:///<database_name>?token=<service token>`
- `create_indexes` - Should unique indexes be created
- `**kwargs` - Additional arguments passed to the destination config

