---
sidebar_label: factory
title: destinations.impl.athena.factory
---

## athena Objects

```python
class athena(Destination[AthenaClientConfiguration, "AthenaClient"])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/athena/factory.py#L106)

### \_\_init\_\_

```python
def __init__(query_result_bucket: t.Optional[str] = None,
             credentials: t.Union[AwsCredentials, t.Dict[str, t.Any],
                                  t.Any] = None,
             athena_work_group: t.Optional[str] = None,
             aws_data_catalog: t.Optional[str] = "awsdatacatalog",
             destination_name: t.Optional[str] = None,
             environment: t.Optional[str] = None,
             **kwargs: t.Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/athena/factory.py#L151)

Configure the Athena destination to use in a pipeline.

All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

**Arguments**:

- `query_result_bucket` - S3 bucket to store query results in
- `credentials` - AWS credentials to connect to the Athena database.
- `athena_work_group` - Athena work group to use
- `aws_data_catalog` - Athena data catalog to use
- `**kwargs` - Additional arguments passed to the destination config

