---
sidebar_label: bigquery
title: destinations.bigquery.bigquery
---

## BigQueryClient Objects

```python
class BigQueryClient(SqlJobClientWithStaging)
```

#### restore\_file\_load

```python
def restore_file_load(file_path: str) -> LoadJob
```

Returns a completed SqlLoadJob or restored BigQueryLoadJob

See base class for details on SqlLoadJob. BigQueryLoadJob is restored with job id derived from `file_path`

**Arguments**:

- `file_path` _str_ - a path to a job file
  

**Returns**:

- `LoadJob` - completed SqlLoadJob or restored BigQueryLoadJob

