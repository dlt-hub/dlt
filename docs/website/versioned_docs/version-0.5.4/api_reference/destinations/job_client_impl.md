---
sidebar_label: job_client_impl
title: destinations.job_client_impl
---

## SqlLoadJob Objects

```python
class SqlLoadJob(LoadJob)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/job_client_impl.py#L69)

A job executing sql statement, without followup trait

## SqlJobClientBase Objects

```python
class SqlJobClientBase(JobClientBase, WithStateSync)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/job_client_impl.py#L137)

### maybe\_ddl\_transaction

```python
@contextlib.contextmanager
def maybe_ddl_transaction() -> Iterator[None]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/job_client_impl.py#L213)

Begins a transaction if sql client supports it, otherwise works in auto commit.

### create\_table\_chain\_completed\_followup\_jobs

```python
def create_table_chain_completed_followup_jobs(
        table_chain: Sequence[TTableSchema]) -> List[NewLoadJob]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/job_client_impl.py#L243)

Creates a list of followup jobs for merge write disposition and staging replace strategies

### start\_file\_load

```python
def start_file_load(table: TTableSchema, file_path: str,
                    load_id: str) -> LoadJob
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/job_client_impl.py#L257)

Starts SqlLoadJob for files ending with .sql or returns None to let derived classes to handle their specific jobs

### restore\_file\_load

```python
def restore_file_load(file_path: str) -> LoadJob
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/job_client_impl.py#L264)

Returns a completed SqlLoadJob or None to let derived classes to handle their specific jobs

Returns completed jobs as SqlLoadJob is executed atomically in start_file_load so any jobs that should be recreated are already completed.
Obviously the case of asking for jobs that were never created will not be handled. With correctly implemented loader that cannot happen.

**Arguments**:

- `file_path` _str_ - a path to a job file
  

**Returns**:

- `LoadJob` - A restored job or none

