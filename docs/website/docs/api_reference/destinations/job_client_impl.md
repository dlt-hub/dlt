---
sidebar_label: job_client_impl
title: destinations.job_client_impl
---

## SqlLoadJob Objects

```python
class SqlLoadJob(LoadJob)
```

A job executing sql statement, without followup trait

## SqlJobClientBase Objects

```python
class SqlJobClientBase(JobClientBase, WithStateSync)
```

#### maybe\_ddl\_transaction

```python
@contextlib.contextmanager
def maybe_ddl_transaction() -> Iterator[None]
```

Begins a transaction if sql client supports it, otherwise works in auto commit

#### create\_table\_chain\_completed\_followup\_jobs

```python
def create_table_chain_completed_followup_jobs(
        table_chain: Sequence[TTableSchema]) -> List[NewLoadJob]
```

Creates a list of followup jobs for merge write disposition and staging replace strategies

#### start\_file\_load

```python
def start_file_load(table: TTableSchema, file_path: str,
                    load_id: str) -> LoadJob
```

Starts SqlLoadJob for files ending with .sql or returns None to let derived classes to handle their specific jobs

#### restore\_file\_load

```python
def restore_file_load(file_path: str) -> LoadJob
```

Returns a completed SqlLoadJob or None to let derived classes to handle their specific jobs

Returns completed jobs as SqlLoadJob is executed atomically in start_file_load so any jobs that should be recreated are already completed.
Obviously the case of asking for jobs that were never created will not be handled. With correctly implemented loader that cannot happen.

**Arguments**:

- `file_path` _str_ - a path to a job file
  

**Returns**:

- `LoadJob` - A restored job or none

## SqlJobClientWithStaging Objects

```python
class SqlJobClientWithStaging(SqlJobClientBase, WithStagingDataset)
```

#### get\_stage\_dispositions

```python
def get_stage_dispositions() -> List[TWriteDisposition]
```

Returns a list of dispositions that require staging tables to be populated

