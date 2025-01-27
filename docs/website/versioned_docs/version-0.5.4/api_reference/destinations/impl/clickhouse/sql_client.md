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

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/clickhouse/sql_client.py#L58)

### drop\_tables

```python
def drop_tables(*tables: str) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/clickhouse/sql_client.py#L149)

Drops a set of tables if they exist

