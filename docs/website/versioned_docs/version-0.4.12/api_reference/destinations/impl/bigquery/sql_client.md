---
sidebar_label: sql_client
title: destinations.impl.bigquery.sql_client
---

## BigQueryDBApiCursorImpl Objects

```python
class BigQueryDBApiCursorImpl(DBApiCursorImpl)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/bigquery/sql_client.py#L43)

Use native BigQuery data frame support if available

### native\_cursor

type: ignore

## BigQuerySqlClient Objects

```python
class BigQuerySqlClient(SqlClientBase[bigquery.Client], DBTransaction)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/bigquery/sql_client.py#L75)

### is\_hidden\_dataset

```python
@property
def is_hidden_dataset() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/bigquery/sql_client.py#L251)

Tells if the dataset associated with sql_client is a hidden dataset.

Hidden datasets are not present in information schema.

