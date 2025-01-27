---
sidebar_label: sql_client
title: destinations.impl.clickhouse.sql_client
---

## ClickHouseSqlClient Objects

```python
class ClickHouseSqlClient(
        SqlClientBase[clickhouse_driver.dbapi.connection.Connection],
        DBTransaction)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/clickhouse/sql_client.py#L66)

### drop\_tables

```python
def drop_tables(*tables: str) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/clickhouse/sql_client.py#L185)

Drops a set of tables if they exist

