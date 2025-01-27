---
sidebar_label: factory
title: destinations.impl.redshift.factory
---

## redshift Objects

```python
class redshift(Destination[RedshiftClientConfiguration, "RedshiftClient"])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/redshift/factory.py#L17)

### \_\_init\_\_

```python
def __init__(credentials: t.Union[RedshiftCredentials, t.Dict[str, t.Any],
                                  str] = None,
             staging_iam_role: t.Optional[str] = None,
             has_case_sensitive_identifiers: bool = False,
             destination_name: t.Optional[str] = None,
             environment: t.Optional[str] = None,
             **kwargs: t.Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/redshift/factory.py#L53)

Configure the Redshift destination to use in a pipeline.

All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

**Arguments**:

- `credentials` - Credentials to connect to the redshift database. Can be an instance of `RedshiftCredentials` or
  a connection string in the format `redshift://user:password@host:port/database`
- `staging_iam_role` - IAM role to use for staging data in S3
- `has_case_sensitive_identifiers` - Are case sensitive identifiers enabled for a database
- `**kwargs` - Additional arguments passed to the destination config

