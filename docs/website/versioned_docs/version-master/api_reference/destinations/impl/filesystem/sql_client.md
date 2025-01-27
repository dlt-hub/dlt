---
sidebar_label: sql_client
title: destinations.impl.filesystem.sql_client
---

## FilesystemSqlClient Objects

```python
class FilesystemSqlClient(DuckDbSqlClient)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/filesystem/sql_client.py#L37)

### memory\_db

Internally created in-mem database in case external is not provided

### create\_views\_for\_tables

```python
@raise_database_error
def create_views_for_tables(tables: Dict[str, str]) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/filesystem/sql_client.py#L204)

Add the required tables as views to the duckdb in memory instance

