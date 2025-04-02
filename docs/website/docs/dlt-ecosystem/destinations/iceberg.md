---
title: Iceberg
description: Iceberg dlt destination
keywords: [iceberg, destination, data warehouse]
---

# Iceberg table format
dlt supports writing [Iceberg](https://iceberg.apache.org/) tables when using the [filesystem](./filesystem.md) destination.

## How it works
dlt uses the [PyIceberg](https://py.iceberg.apache.org/) library to write Iceberg tables. One or multiple Parquet files are prepared during the extract and normalize steps. In the load step, these Parquet files are exposed as an Arrow data structure and fed into `pyiceberg`.

## Iceberg single-user ephemeral catalog
dlt uses single-table, ephemeral, in-memory, SQLite-based [Iceberg catalogs](https://iceberg.apache.org/terms/#catalog). These catalogs are created "on demand" when a pipeline is run, and do not persist afterwards. If a table already exists in the filesystem, it gets registered into the catalog using its latest metadata file. This allows for a serverless setup. It is currently not possible to connect your own Iceberg catalog.

:::caution
While ephemeral catalogs make it easy to get started with Iceberg, it comes with limitations:
- concurrent writes are not handled and may lead to corrupt table state
- we cannot guarantee that reads concurrent with writes are clean
- the latest manifest file needs to be searched for using file listing—this can become slow with large tables, especially in cloud object stores
:::

:::tip dlt+
If you're interested in a multi-user cloud experience and integration with vendor catalogs, such as Polaris or Unity Catalog, check out [dlt+](../../plus/ecosystem/iceberg.md).
:::

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

Set the `table_format` argument to `iceberg` when defining your resource:

```py
@dlt.resource(table_format="iceberg")
def my_iceberg_resource():
    ...
```

or when calling `run` on your pipeline:

```py
pipeline.run(my_resource, table_format="iceberg")
```

:::note
dlt always uses Parquet as `loader_file_format` when using the `iceberg` table format. Any setting of `loader_file_format` is disregarded.
:::

## Table format partitioning
Iceberg tables can be partitioned by specifying one or more `partition` column hints. This example partitions an Iceberg table by the `foo` column:

```py
@dlt.resource(
  table_format="iceberg",
  columns={"foo": {"partition": True}}
)
def my_iceberg_resource():
    ...
```

:::note
Iceberg uses [hidden partioning](https://iceberg.apache.org/docs/latest/partitioning/).
:::

:::caution
Partition evolution (changing partition columns after a table has been created) is not supported.
:::

## Table access helper functions
You can use the `get_iceberg_tables` helper function to access native table objects. These are `pyiceberg` [Table](https://py.iceberg.apache.org/reference/pyiceberg/table/#pyiceberg.table.Table) objects.

```py
from dlt.common.libs.pyiceberg import get_iceberg_tables

# get dictionary of Table objects
iceberg_tables = get_iceberg_tables(pipeline)

# execute operations on Table objects
# etc.
```

## Google Cloud Storage authentication

Note that not all authentication methods are supported when using Iceberg on Google Cloud Storage:

- [OAuth](../destinations/bigquery.md#oauth-20-authentication) - ✅ Supported
- [Service Account](bigquery.md#setup-guide) - ❌ Not supported
- [Application Default Credentials](bigquery.md#using-default-credentials) - ❌ Not supported

:::note
The [S3-compatible](./filesystem.md#using-s3-compatible-storage) interface for Google Cloud Storage is not supported when using `iceberg`.
:::

## Iceberg Azure scheme
The `az` [scheme](./filesystem.md#supported-schemes) is not supported when using the `iceberg` table format. Please use the `abfss` scheme. This is because `pyiceberg`, which dlt used under the hood, currently does not support `az`.

## Table format `merge` support
The `merge` write disposition is not supported for Iceberg and falls back to `append`. If you're interested in support for the `merge` write disposition with Iceberg, check out [dlt+ Iceberg destination](../../plus/ecosystem/iceberg.md).