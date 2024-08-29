---
sidebar_label: factory
title: destinations.impl.databricks.factory
---

## databricks Objects

```python
class databricks(Destination[DatabricksClientConfiguration,
                             "DatabricksClient"])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/databricks/factory.py#L15)

### \_\_init\_\_

```python
def __init__(credentials: t.Union[DatabricksCredentials, t.Dict[str, t.Any],
                                  str] = None,
             destination_name: t.Optional[str] = None,
             environment: t.Optional[str] = None,
             **kwargs: t.Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/databricks/factory.py#L27)

Configure the Databricks destination to use in a pipeline.

All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

**Arguments**:

- `credentials` - Credentials to connect to the databricks database. Can be an instance of `DatabricksCredentials` or
  a connection string in the format `databricks://user:password@host:port/database`
- `**kwargs` - Additional arguments passed to the destination config

