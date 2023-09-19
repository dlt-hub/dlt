---
sidebar_label: athena
title: destinations.athena.athena
---

## DoNothingJob Objects

```python
class DoNothingJob(LoadJob)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/athena/athena.py#L96)

The most lazy class of dlt

## AthenaClient Objects

```python
class AthenaClient(SqlJobClientBase)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/athena/athena.py#L251)

#### start\_file\_load

```python
def start_file_load(table: TTableSchema, file_path: str,
                    load_id: str) -> LoadJob
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/athena/athena.py#L322)

Starts SqlLoadJob for files ending with .sql or returns None to let derived classes to handle their specific jobs

