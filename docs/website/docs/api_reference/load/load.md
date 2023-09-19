---
sidebar_label: load
title: load.load
---

## Load Objects

```python
class Load(Runnable[ThreadPool])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/load/load.py#L32)

#### maybe\_with\_staging\_dataset

```python
@contextlib.contextmanager
def maybe_with_staging_dataset(job_client: JobClientBase,
                               table: TTableSchema) -> Iterator[None]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/load/load.py#L95)

Executes job client methods in context of staging dataset if `table` has `write_disposition` that requires it

#### get\_completed\_table\_chain

```python
def get_completed_table_chain(
        load_id: str,
        schema: Schema,
        top_merged_table: TTableSchema,
        being_completed_job_id: str = None) -> List[TTableSchema]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/load/load.py#L184)

Gets a table chain starting from the `top_merged_table` containing only tables with completed/failed jobs. None is returned if there's any job that is not completed

Optionally `being_completed_job_id` can be passed that is considered to be completed before job itself moves in storage

#### get\_table\_chain\_tables\_for\_write\_disposition

```python
def get_table_chain_tables_for_write_disposition(
        load_id: str, schema: Schema,
        dispositions: List[TWriteDisposition]) -> Set[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/load/load.py#L281)

Get all jobs for tables with given write disposition and resolve the table chain

