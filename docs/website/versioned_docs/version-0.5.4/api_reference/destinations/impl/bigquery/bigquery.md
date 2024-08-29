---
sidebar_label: bigquery
title: destinations.impl.bigquery.bigquery
---

## BigQueryClient Objects

```python
class BigQueryClient(SqlJobClientWithStaging, SupportsStagingDestination)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/bigquery/bigquery.py#L167)

### restore\_file\_load

```python
def restore_file_load(file_path: str) -> LoadJob
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/bigquery/bigquery.py#L186)

Returns a completed SqlLoadJob or restored BigQueryLoadJob

See base class for details on SqlLoadJob.
BigQueryLoadJob is restored with a job ID derived from `file_path`.

**Arguments**:

- `file_path` _str_ - a path to a job file.
  

**Returns**:

- `LoadJob` - completed SqlLoadJob or restored BigQueryLoadJob

