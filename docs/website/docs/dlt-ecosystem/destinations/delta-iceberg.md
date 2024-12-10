---
title: Delta / Iceberg
description: Delta / Iceberg `dlt` destination
keywords: [delta, iceberg, destination, data warehouse]
---

# Delta and Iceberg table formats
`dlt` supports writing [Delta](https://delta.io/) and [Iceberg](https://iceberg.apache.org/) tables when using the [filesystem](./filesystem.md) destination.

## How it works
`dlt` uses the [deltalake](https://pypi.org/project/deltalake/) and [pyiceberg](https://pypi.org/project/pyiceberg/) libraries to write Delta and Iceberg tables, respectively. One or multiple Parquet files are prepared during the extract and normalize steps. In the load step, these Parquet files are exposed as an Arrow data structure and fed into `deltalake` or `pyiceberg`.

## Iceberg single-user ephemeral catalog
`dlt` uses single-table, ephemeral, in-memory, sqlite-based [Iceberg catalog](https://iceberg.apache.org/concepts/catalog/)s. These catalogs are created "on demand" when a pipeline is run, and do not persist afterwards. If a table already exists in the filesystem, it gets registered into the catalog using its latest metadata file. This allows for a serverless setup. It is currently not possible to connect your own Iceberg catalog.

:::caution
While ephemeral catalogs make it easy to get started with Iceberg, it comes with limitations:
- concurrent writes are not handled and may lead to corrupt table state
- we cannot guarantee that reads concurrent with writes are clean
- the latest manifest file needs to be searched for using file listing—this can become slow with large tables, especially in cloud object stores
:::

## Delta dependencies

You need the `deltalake` package to use this format:

```sh
pip install "dlt[deltalake]"
```

You also need `pyarrow>=17.0.0`:

```sh
pip install 'pyarrow>=17.0.0'
```

## Iceberg dependencies

You need Python version 3.9 or higher and the `pyiceberg` package to use this format:

```sh
pip install "dlt[pyiceberg]"
```

You also need `sqlalchemy>=2.0.18`:

```sh
pip install 'sqlalchemy>=2.0.18'
```

## Set table format

Set the `table_format` argument to `delta` or `iceberg` when defining your resource:

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
`dlt` always uses Parquet as `loader_file_format` when using the `delta` or `iceberg` table format. Any setting of `loader_file_format` is disregarded.
:::


## Table format partitioning
Both `delta` and `iceberg` tables can be partitioned by specifying one or more `partition` column hints. This example partitions a Delta table by the `foo` column:

```py
@dlt.resource(
  table_format="delta",
  columns={"foo": {"partition": True}}
)
def my_delta_resource():
    ...
```

:::note
Delta uses [Hive-style partitioning](https://delta.io/blog/pros-cons-hive-style-partionining/), while Iceberg uses [hidden partioning](https://iceberg.apache.org/docs/latest/partitioning/).
:::

:::caution
Partition evolution (changing partition columns after a table has been created) is not supported.
:::

## Table access helper functions
You can use the `get_delta_tables` and `get_iceberg_tables` helper functions to acccess native table objects. For `delta` these are `deltalake` [DeltaTable](https://delta-io.github.io/delta-rs/api/delta_table/) objects, for `iceberg` these are `pyiceberg` [Table](https://py.iceberg.apache.org/reference/pyiceberg/table/#pyiceberg.table.Table) objects.

```py
from dlt.common.libs.deltalake import get_delta_tables
# from dlt.common.libs.pyiceberg import get_iceberg_tables

...

# get dictionary of DeltaTable objects
delta_tables = get_delta_tables(pipeline)

# execute operations on DeltaTable objects
delta_tables["my_delta_table"].optimize.compact()
delta_tables["another_delta_table"].optimize.z_order(["col_a", "col_b"])
# delta_tables["my_delta_table"].vacuum()
# etc.
```

## Table format Google Cloud Storage authentication

Note that not all authentication methods are supported when using table formats on Google Cloud Storage:

| Authentication method | `delta` | `iceberg` |
| -- | -- | -- |
| [Service Account](bigquery.md#setup-guide) | ✅ | ❌ |
| [OAuth](../destinations/bigquery.md#oauth-20-authentication) | ❌ | ✅ |
| [Application Default Credentials](bigquery.md#using-default-credentials) | ✅ | ❌ |

:::note
The [S3-compatible](#using-s3-compatible-storage) interface for Google Cloud Storage is not supported when using `iceberg`.
:::

## Iceberg Azure scheme
The `az` [scheme](#supported-schemes) is not supported when using the `iceberg` table format. Please use the `abfss` scheme. This is because `pyiceberg`, which `dlt` used under the hood, currently does not support `az`.

## Table format `merge` support (**experimental**)
The [`upsert`](../../general-usage/incremental-loading.md#upsert-strategy) merge strategy is supported for `delta`. For `iceberg`, the `merge` write disposition is not supported and falls back to `append`.

:::caution
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
...
```

### Known limitations
- `hard_delete` hint not supported
- Deleting records from nested tables not supported
  - This means updates to JSON columns that involve element removals are not propagated. For example, if you first load `{"key": 1, "nested": [1, 2]}` and then load `{"key": 1, "nested": [1]}`, then the record for element `2` will not be deleted from the nested table.

## Delta table format storage options
You can pass storage options by configuring `destination.filesystem.deltalake_storage_options`:

```toml
[destination.filesystem]
deltalake_storage_options = '{"AWS_S3_LOCKING_PROVIDER": "dynamodb", "DELTA_DYNAMO_TABLE_NAME": "custom_table_name"}'
```

`dlt` passes these options to the `storage_options` argument of the `write_deltalake` method in the `deltalake` library. Look at their [documentation](https://delta-io.github.io/delta-rs/api/delta_writer/#deltalake.write_deltalake) to see which options can be used.

You don't need to specify credentials here. `dlt` merges the required credentials with the options you provided before passing it as `storage_options`.

>❗When using `s3`, you need to specify storage options to [configure](https://delta-io.github.io/delta-rs/usage/writing/writing-to-s3-with-locking-provider/) locking behavior.

## Delta table format memory usage
:::caution
Beware that when loading a large amount of data for one table, the underlying rust implementation will consume a lot of memory. This is a known issue and the maintainers are actively working on a solution. You can track the progress [here](https://github.com/delta-io/delta-rs/pull/2289). Until the issue is resolved, you can mitigate the memory consumption by doing multiple smaller incremental pipeline runs.
:::