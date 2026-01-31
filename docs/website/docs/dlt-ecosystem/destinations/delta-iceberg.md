---
title: Delta
description: Delta dlt destination
keywords: [delta, destination, data warehouse]
---

# Delta table format
dlt supports writing [Delta](https://delta.io/) tables when using the [filesystem](./filesystem.md) destination.

## How it works
dlt uses the [deltalake](https://pypi.org/project/deltalake/) library to write Delta tables. One or multiple Parquet files are prepared during the extract and normalize steps. In the load step, these Parquet files are exposed as an Arrow data structure and fed into `deltalake`.

## Delta dependencies

You need the `deltalake` package to use this format:

```sh
pip install "dlt[deltalake]"
```

You also need `pyarrow>=17.0.0`:

```sh
pip install 'pyarrow>=17.0.0'
```

## Set table format

Set the `table_format` argument to `delta` when defining your resource:

```py
@dlt.resource(table_format="delta")
def my_delta_resource():
    ...
```

or when calling `run` on your pipeline:

```py
pipeline.run(my_resource, table_format="delta")
```

:::note
dlt always uses Parquet as `loader_file_format` when using the `delta` table format. Any setting of `loader_file_format` is disregarded.
:::

## Table format partitioning
Delta tables can be partitioned by specifying one or more `partition` column hints. This example partitions a Delta table by the `foo` column:

```py
@dlt.resource(
  table_format="delta",
  columns={"foo": {"partition": True}}
)
def my_delta_resource():
    ...
```

:::note
Delta uses [Hive-style partitioning](https://delta.io/blog/pros-cons-hive-style-partionining/).
:::

:::warning
Partition evolution (changing partition columns after a table has been created) is not supported.
:::

## Table access helper functions
You can use the `get_delta_tables` helper function to access native table objects. These are `deltalake` [DeltaTable](https://delta-io.github.io/delta-rs/api/delta_table/) objects.

```py
from dlt.common.libs.deltalake import get_delta_tables

# get dictionary of DeltaTable objects
delta_tables = get_delta_tables(pipeline)

# execute operations on DeltaTable objects
delta_tables["my_delta_table"].optimize.compact()
delta_tables["another_delta_table"].optimize.z_order(["col_a", "col_b"])
# delta_tables["my_delta_table"].vacuum()
# etc.
```

## Google Cloud Storage authentication

Note that not all authentication methods are supported when using Delta table format on Google Cloud Storage:

- [Service Account](bigquery.md#setup-guide) - ✅ Supported
- [Application Default Credentials](bigquery.md#using-default-credentials) - ✅ Supported
- [OAuth](../destinations/bigquery.md#oauth-20-authentication) - ❌ Not supported

## Table format `merge` support (**experimental**)
The [`upsert`](../../general-usage/merge-loading.md#upsert-strategy) merge strategy is supported for `delta`.

:::warning
The `upsert` merge strategy for the filesystem destination with Delta table format is **experimental**.
:::

```py
@dlt.resource(
    write_disposition={"disposition": "merge", "strategy": "upsert"},
    primary_key="my_primary_key",
    table_format="delta"
)
def my_upsert_resource():
    ...
```

### Known limitations
- `hard_delete` hint not supported
- Deleting records from nested tables not supported
  - This means updates to JSON columns that involve element removals are not propagated. For example, if you first load `{"key": 1, "nested": [1, 2]}` and then load `{"key": 1, "nested": [1]}`, then the record for element `2` will not be deleted from the nested table.

By default, dlt runs Delta table upserts in streamed mode to reduce memory pressure. To enable the use of source table statistics to derive an early pruning predicate, set:

```toml
[destination.filesystem]
deltalake_streamed_exec = false
```

## Delta table format storage options and configuration
You can pass storage options and configuration by configuring both `destination.filesystem.deltalake_storage_options` and
`destination.filesystem.deltalake_configuration`:

```toml
[destination.filesystem]
deltalake_configuration = '{"delta.enableChangeDataFeed": "true", "delta.minWriterVersion": "7"}'
deltalake_storage_options = '{"AWS_S3_LOCKING_PROVIDER": "dynamodb", "DELTA_DYNAMO_TABLE_NAME": "custom_table_name"}'
```

dlt passes these as arguments to the `write_deltalake` method in the `deltalake` library (`deltalake_configuration` maps to `configuration` and `deltalake_storage_options` maps to `storage_options`). Look at their [documentation](https://delta-io.github.io/delta-rs/api/delta_writer/#deltalake.write_deltalake) to see which options can be used.

You don't need to specify credentials here. dlt merges the required credentials with the options you provided before passing it as `storage_options`.

>❗When using `s3`, you need to specify storage options to [configure](https://delta-io.github.io/delta-rs/usage/writing/writing-to-s3-with-locking-provider/) locking behavior.

## Delta table format memory usage
:::warning
Beware that when loading a large amount of data for one table, the underlying rust implementation will consume a lot of memory. This is a known issue and the maintainers are actively working on a solution. You can track the progress [here](https://github.com/delta-io/delta-rs/pull/2289). Until the issue is resolved, you can mitigate the memory consumption by doing multiple smaller incremental pipeline runs.
:::
