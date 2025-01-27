---
sidebar_label: factory
title: destinations.impl.dremio.factory
---

## dremio Objects

```python
class dremio(Destination[DremioClientConfiguration, "DremioClient"])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/dremio/factory.py#L83)

### \_\_init\_\_

```python
def __init__(credentials: t.Union[DremioCredentials, t.Dict[str, t.Any],
                                  str] = None,
             staging_data_source: str = None,
             destination_name: t.Optional[str] = None,
             environment: t.Optional[str] = None,
             **kwargs: t.Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/dremio/factory.py#L121)

Configure the Dremio destination to use in a pipeline.

All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

**Arguments**:

- `credentials` - Credentials to connect to the dremio database. Can be an instance of `DremioCredentials` or
  a connection string in the format `dremio://user:password@host:port/database`
- `staging_data_source` - The name of the "Object Storage" data source in Dremio containing the s3 bucket

