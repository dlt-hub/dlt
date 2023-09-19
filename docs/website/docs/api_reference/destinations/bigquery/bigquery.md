---
sidebar_label: bigquery
title: destinations.bigquery.bigquery
---

## BigQueryClient Objects

```python
class BigQueryClient(SqlJobClientWithStaging)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/bigquery/bigquery.py#L134)

#### restore\_file\_load

```python
def restore_file_load(file_path: str) -> LoadJob
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/bigquery/bigquery.py#L156)

Returns a completed SqlLoadJob or restored BigQueryLoadJob

See base class for details on SqlLoadJob. BigQueryLoadJob is restored with job id derived from `file_path`

**Arguments**:

- `file_path` _str_ - a path to a job file
  

**Returns**:

- `LoadJob` - completed SqlLoadJob or restored BigQueryLoadJob

