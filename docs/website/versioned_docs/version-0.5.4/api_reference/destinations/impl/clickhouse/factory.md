---
sidebar_label: factory
title: destinations.impl.clickhouse.factory
---

## clickhouse Objects

```python
class clickhouse(Destination[ClickHouseClientConfiguration,
                             "ClickHouseClient"])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/clickhouse/factory.py#L22)

### \_\_init\_\_

```python
def __init__(credentials: t.Union[ClickHouseCredentials, str, t.Dict[str,
                                                                     t.Any],
                                  t.Type["Connection"]] = None,
             destination_name: str = None,
             environment: str = None,
             **kwargs: t.Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/clickhouse/factory.py#L79)

Configure the ClickHouse destination to use in a pipeline.

All arguments provided here supersede other configuration sources such as environment
variables and dlt config files.

**Arguments**:

- `credentials` - Credentials to connect to the clickhouse database.
  Can be an instance of `ClickHouseCredentials`, or a connection string
  in the format `clickhouse://user:password@host:port/database`.
- `**kwargs` - Additional arguments passed to the destination config.

