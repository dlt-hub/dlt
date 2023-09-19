---
sidebar_label: athena
title: destinations.athena.athena
---

## DoNothingJob Objects

```python
class DoNothingJob(LoadJob)
```

The most lazy class of dlt

## AthenaClient Objects

```python
class AthenaClient(SqlJobClientBase)
```

#### start\_file\_load

```python
def start_file_load(table: TTableSchema, file_path: str,
                    load_id: str) -> LoadJob
```

Starts SqlLoadJob for files ending with .sql or returns None to let derived classes to handle their specific jobs

