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

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/clickhouse/sql_client.py#L58)

### drop\_tables

```python
def drop_tables(*tables: str) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/clickhouse/sql_client.py#L149)

Drops a set of tables if they exist

