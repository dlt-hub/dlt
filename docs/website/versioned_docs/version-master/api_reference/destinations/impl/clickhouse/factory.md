---
sidebar_label: factory
title: destinations.impl.clickhouse.factory
---

## clickhouse Objects

```python
class clickhouse(Destination[ClickHouseClientConfiguration,
                             "ClickHouseClient"])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/clickhouse/factory.py#L93)

### \_\_init\_\_

```python
def __init__(credentials: t.Union[ClickHouseCredentials, str, t.Dict[str,
                                                                     t.Any],
                                  t.Type["Connection"]] = None,
             destination_name: str = None,
             environment: str = None,
             **kwargs: t.Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/clickhouse/factory.py#L152)

Configure the ClickHouse destination to use in a pipeline.

All arguments provided here supersede other configuration sources such as environment
variables and dlt config files.

**Arguments**:

- `credentials` - Credentials to connect to the clickhouse database.
  Can be an instance of `ClickHouseCredentials`, or a connection string
  in the format `clickhouse://user:password@host:port/database`.
- `**kwargs` - Additional arguments passed to the destination config.

