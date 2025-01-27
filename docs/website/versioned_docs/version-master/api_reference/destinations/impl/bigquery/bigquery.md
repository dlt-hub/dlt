---
sidebar_label: bigquery
title: destinations.impl.bigquery.bigquery
---

## BigQueryClient Objects

```python
class BigQueryClient(SqlJobClientWithStagingDataset,
                     SupportsStagingDestination)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/bigquery/bigquery.py#L174)

### get\_storage\_tables

```python
def get_storage_tables(
        table_names: Iterable[str]
) -> Iterable[Tuple[str, TTableSchemaColumns]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/bigquery/bigquery.py#L353)

Gets table schemas from BigQuery using INFORMATION_SCHEMA or get_table for hidden datasets

