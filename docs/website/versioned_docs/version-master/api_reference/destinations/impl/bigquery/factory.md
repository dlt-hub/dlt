---
sidebar_label: factory
title: destinations.impl.bigquery.factory
---

## bigquery Objects

```python
class bigquery(Destination[BigQueryClientConfiguration, "BigQueryClient"])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/bigquery/factory.py#L90)

### \_\_init\_\_

```python
def __init__(credentials: t.Optional[GcpServiceAccountCredentials] = None,
             location: t.Optional[str] = None,
             has_case_sensitive_identifiers: bool = None,
             destination_name: t.Optional[str] = None,
             environment: t.Optional[str] = None,
             **kwargs: t.Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/bigquery/factory.py#L135)

Configure the MsSql destination to use in a pipeline.

All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

**Arguments**:

- `credentials` - Credentials to connect to the mssql database. Can be an instance of `GcpServiceAccountCredentials` or
  a dict or string with service accounts credentials as used in the Google Cloud
- `location` - A location where the datasets will be created, eg. "EU". The default is "US"
- `has_case_sensitive_identifiers` - Is the dataset case-sensitive, defaults to True
- `**kwargs` - Additional arguments passed to the destination config

