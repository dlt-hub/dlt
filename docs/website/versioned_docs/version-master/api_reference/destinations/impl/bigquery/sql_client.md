---
sidebar_label: sql_client
title: destinations.impl.bigquery.sql_client
---

## BigQueryDBApiCursorImpl Objects

```python
class BigQueryDBApiCursorImpl(DBApiCursorImpl)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/bigquery/sql_client.py#L44)

Use native BigQuery data frame support if available

### native\_cursor

type: ignore

## BigQuerySqlClient Objects

```python
class BigQuerySqlClient(SqlClientBase[bigquery.Client], DBTransaction)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/bigquery/sql_client.py#L59)

### is\_hidden\_dataset

```python
@property
def is_hidden_dataset() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/bigquery/sql_client.py#L235)

Tells if the dataset associated with sql_client is a hidden dataset.

Hidden datasets are not present in information schema.

### truncate\_tables\_if\_exist

```python
def truncate_tables_if_exist(*tables: str) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/bigquery/sql_client.py#L276)

NOTE: We only truncate tables that exist, for auto-detect schema we don't know which tables exist

