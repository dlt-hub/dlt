---
sidebar_label: factory
title: destinations.impl.redshift.factory
---

## redshift Objects

```python
class redshift(Destination[RedshiftClientConfiguration, "RedshiftClient"])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/redshift/factory.py#L15)

### \_\_init\_\_

```python
def __init__(credentials: t.Union[RedshiftCredentials, t.Dict[str, t.Any],
                                  str] = None,
             create_indexes: bool = True,
             staging_iam_role: t.Optional[str] = None,
             destination_name: t.Optional[str] = None,
             environment: t.Optional[str] = None,
             **kwargs: t.Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/redshift/factory.py#L27)

Configure the Redshift destination to use in a pipeline.

All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

**Arguments**:

- `credentials` - Credentials to connect to the redshift database. Can be an instance of `RedshiftCredentials` or
  a connection string in the format `redshift://user:password@host:port/database`
- `create_indexes` - Should unique indexes be created
- `staging_iam_role` - IAM role to use for staging data in S3
- `**kwargs` - Additional arguments passed to the destination config

