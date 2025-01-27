---
sidebar_label: factory
title: destinations.impl.postgres.factory
---

## postgres Objects

```python
class postgres(Destination[PostgresClientConfiguration, "PostgresClient"])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/postgres/factory.py#L125)

### \_\_init\_\_

```python
def __init__(credentials: t.Union[PostgresCredentials, t.Dict[str, t.Any],
                                  str] = None,
             create_indexes: bool = True,
             csv_format: t.Optional[CsvFormatConfiguration] = None,
             destination_name: t.Optional[str] = None,
             environment: t.Optional[str] = None,
             **kwargs: t.Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/postgres/factory.py#L167)

Configure the Postgres destination to use in a pipeline.

All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

**Arguments**:

- `credentials` - Credentials to connect to the postgres database. Can be an instance of `PostgresCredentials` or
  a connection string in the format `postgres://user:password@host:port/database`
- `create_indexes` - Should unique indexes be created
- `csv_format` - Formatting options for csv file format
- `**kwargs` - Additional arguments passed to the destination config

