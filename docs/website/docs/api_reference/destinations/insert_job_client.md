---
sidebar_label: insert_job_client
title: destinations.insert_job_client
---

## InsertValuesJobClient Objects

```python
class InsertValuesJobClient(SqlJobClientWithStaging)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/insert_job_client.py#L92)

#### restore\_file\_load

```python
def restore_file_load(file_path: str) -> LoadJob
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/insert_job_client.py#L94)

Returns a completed SqlLoadJob or InsertValuesJob

Returns completed jobs as SqlLoadJob and InsertValuesJob executed atomically in start_file_load so any jobs that should be recreated are already completed.
Obviously the case of asking for jobs that were never created will not be handled. With correctly implemented loader that cannot happen.

**Arguments**:

- `file_path` _str_ - a path to a job file
  

**Returns**:

- `LoadJob` - Always a restored job completed

