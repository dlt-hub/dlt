---
sidebar_label: bigquery
title: destinations.impl.bigquery.bigquery
---

## BigQueryClient Objects

```python
class BigQueryClient(SqlJobClientWithStaging, SupportsStagingDestination)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/bigquery/bigquery.py#L215)

### get\_storage\_tables

```python
def get_storage_tables(
        table_names: Iterable[str]
) -> Iterable[Tuple[str, TTableSchemaColumns]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/bigquery/bigquery.py#L370)

Gets table schemas from BigQuery using INFORMATION_SCHEMA or get_table for hidden datasets

