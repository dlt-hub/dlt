---
sidebar_label: job_client_impl
title: destinations.job_client_impl
---

## SqlLoadJob Objects

```python
class SqlLoadJob(RunnableLoadJob)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/job_client_impl.py#L71)

A job executing sql statement, without followup trait

## SqlJobClientBase Objects

```python
class SqlJobClientBase(JobClientBase, WithStateSync)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/job_client_impl.py#L124)

### INFO\_TABLES\_QUERY\_THRESHOLD

Fallback to querying all tables in the information schema if checking more than threshold

### drop\_tables

```python
def drop_tables(*tables: str, delete_schema: bool = True) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/job_client_impl.py#L189)

Drop tables in destination database and optionally delete the stored schema as well.
Clients that support ddl transactions will have both operations performed in a single transaction.

**Arguments**:

- `tables` - Names of tables to drop.
- `delete_schema` - If True, also delete all versions of the current schema from storage

### maybe\_ddl\_transaction

```python
@contextlib.contextmanager
def maybe_ddl_transaction() -> Iterator[None]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/job_client_impl.py#L203)

Begins a transaction if sql client supports it, otherwise works in auto commit.

### create\_table\_chain\_completed\_followup\_jobs

```python
def create_table_chain_completed_followup_jobs(
    table_chain: Sequence[TTableSchema],
    completed_table_chain_jobs: Optional[Sequence[LoadJobInfo]] = None
) -> List[FollowupJobRequest]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/job_client_impl.py#L239)

Creates a list of followup jobs for merge write disposition and staging replace strategies

### create\_load\_job

```python
def create_load_job(table: TTableSchema,
                    file_path: str,
                    load_id: str,
                    restore: bool = False) -> LoadJob
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/job_client_impl.py#L257)

Starts SqlLoadJob for files ending with .sql or returns None to let derived classes to handle their specific jobs

### get\_storage\_tables

```python
def get_storage_tables(
        table_names: Iterable[str]
) -> Iterable[Tuple[str, TTableSchemaColumns]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/job_client_impl.py#L287)

Uses INFORMATION_SCHEMA to retrieve table and column information for tables in `table_names` iterator.
Table names should be normalized according to naming convention and will be further converted to desired casing
in order to (in most cases) create case-insensitive name suitable for search in information schema.

The column names are returned as in information schema. To match those with columns in existing table, you'll need to use
`schema.get_new_table_columns` method and pass the correct casing. Most of the casing function are irreversible so it is not
possible to convert identifiers into INFORMATION SCHEMA back into case sensitive dlt schema.

### get\_storage\_table

```python
def get_storage_table(table_name: str) -> Tuple[bool, TTableSchemaColumns]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/job_client_impl.py#L370)

Uses get_storage_tables to get single `table_name` schema.

Returns (True, ...) if table exists and (False, {}) when not

