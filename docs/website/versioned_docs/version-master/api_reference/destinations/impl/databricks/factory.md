---
sidebar_label: factory
title: destinations.impl.databricks.factory
---

## databricks Objects

```python
class databricks(Destination[DatabricksClientConfiguration,
                             "DatabricksClient"])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/databricks/factory.py#L105)

### \_\_init\_\_

```python
def __init__(credentials: t.Union[DatabricksCredentials, t.Dict[str, t.Any],
                                  str] = None,
             is_staging_external_location: t.Optional[bool] = False,
             staging_credentials_name: t.Optional[str] = None,
             destination_name: t.Optional[str] = None,
             environment: t.Optional[str] = None,
             **kwargs: t.Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/databricks/factory.py#L150)

Configure the Databricks destination to use in a pipeline.

All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

**Arguments**:

- `credentials` - Credentials to connect to the databricks database. Can be an instance of `DatabricksCredentials` or
  a connection string in the format `databricks://user:password@host:port/database`
- `is_staging_external_location` - If true, the temporary credentials are not propagated to the COPY command
- `staging_credentials_name` - If set, credentials with given name will be used in copy command
- `**kwargs` - Additional arguments passed to the destination config

