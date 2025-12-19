---
title: Iceberg
description: Iceberg dlt destination
keywords: [iceberg, destination, data warehouse]
---

# Iceberg table format
dlt supports writing [Iceberg](https://iceberg.apache.org/) tables when using the [filesystem](./filesystem.md) destination.

## How it works
dlt uses the [PyIceberg](https://py.iceberg.apache.org/) library to write Iceberg tables. One or multiple Parquet files are prepared during the extract and normalize steps. In the load step, these Parquet files are exposed as an Arrow data structure and fed into `pyiceberg`.

## Iceberg catalogs support
dlt leverages `pyiceberg`'s `load_catalog` function to be able to work with the same catalogs that `pyiceberg` would support, including `REST` and `SQL` catalogs. This includes using single-table, ephemeral, in-memory, SQLite-based catalogs. For more information on how `pyiceberg` works with catalogs, reference [their documentation](https://py.iceberg.apache.org/). To enable this, dlt either translates the configuration in the `secrets.toml` and `config.toml` into a valid `pyiceberg` config, or it delegates `pyiceberg` the task of resolving the needed configuration. 

## Iceberg dependencies

You need Python version 3.9 or higher and the `pyiceberg` package to use this format:

```sh
pip install "dlt[pyiceberg]"
```

You also need `sqlalchemy>=2.0.18`:

```sh
pip install 'sqlalchemy>=2.0.18'
```
## Additional permissions for Iceberg

When using Iceberg with object stores like S3, additional permissions may be required for operations like multipart uploads and tagging. Make sure your IAM role or user has the following permissions:

```json
[
  "s3:ListBucketMultipartUploads",
  "s3:GetBucketLocation",
  "s3:AbortMultipartUpload",
  "s3:PutObjectTagging",
  "s3:GetObjectTagging"
]
```

## Set the config for your catalog and, if required, your storage config.

This is applicable only if you already have a catalog set-up. If you want to use ephemeral catalogs, you can skip this section, dlt will default to it. 

dlt allows you to either provide `pyiceberg`'s config through dlt's config mechanisms (`secrets.toml` or environment variables), or through the `pyiceberg` mechanisms supported by `load_catalog` (i.e. `.pyiceberg.yaml` or `pyiceberg`'s env vars). To better learn how to set up `.pyiceberg.yaml` or to learn how `pyiceberg` utilizes env vars, please visit their documentation [here](https://py.iceberg.apache.org/configuration/). In the remaining of this doc snippet we will cover how to set it up through dlt's config mechanisms. 

In the back, dlt utilizes `load_catalog`, so we try to keep the config we will pass along, as close as possible to the `pyiceberg` one. Specifically, we want to provide a catalog name and catalog type (`rest` or `sql`), and we provide them under the `iceberg_catalog` section of the `secrets.toml` or the env vars. These two variables will be used either to detect the catalog we want to load (for example, you have a `.pyiceberg.yaml` with multiple catalogs), and to do some validations when required. 

```toml
[iceberg_catalog]
iceberg_catalog_name = "default"
iceberg_catalog_type = "rest"
```
or 
```sh
export DLT_ICEBERG_CATALOG_NAME=default
export DLT_ICEBERG_CATALOG_TYPE=rest
```

If we don't provide these variables they will default to `iceberg_catalog_name = 'default'` and `iceberg_catalog_type = 'sql'`. 


On top of this we will always require to provide a catalog configuration, either through dlt or through `pyiceberg`, dlt attempts to load your catalog in the following priority order:
1. **Explicit config from `secrets.toml`** (highest priority) - If you provide `iceberg_catalog.iceberg_catalog_config` in your `secrets.toml`, dlt will use this configuration
2. **PyIceberg's standard mechanisms** - If no explicit config is found, dlt delegates to `pyiceberg`'s `load_catalog`, which searches for `.pyiceberg.yaml` or `PYICEBERG_CATALOG_*` environment variables
3. **Ephemeral SQLite catalog** (fallback) - If no configuration is found, dlt creates an in-memory SQLite catalog for backward compatibility and creates the configuration

In some cases, we will want the storage configuration as well (for instance, if you are not using `vended-credential` and instead you are using `remote-signing`). Let's start with the catalog configuration, which we store under `iceberg_catalog.iceberg_catalog_config`: 

```toml
[iceberg_catalog.iceberg_catalog_config]
uri = "http://localhost:8181/catalog" # DLT_ICEBERG_CATALOG__ICEBERG_CATALOG_CONFIG__URI
type = "rest" # DLT_ICEBERG_CATALOG__ICEBERG_CATALOG_CONFIG__TYPE
warehouse = "default" # DLT_ICEBERG_CATALOG__ICEBERG_CATALOG_CONFIG__WAREHOUSE
```

In this case we are using a `rest` catalog located in `localhost:8181` and we look for the default warehouse. These configs are passed in the same way and names as `pyiceberg` expects, which makes it easier to pass new configuration even if it is not in this snippet. 

So let's continue now with storage. In some cases your catalog and storage may be able to handle `vended-credentials`, in that case you don't need to include the storage credentials here as the `vended-credentials` process will allow the catalog to temporarily generate those credentials. However, if you use `remote-signing` we need to provide them, and our configuration would become something like this: 

```toml
[iceberg_catalog.iceberg_catalog_config]
uri = "http://localhost:8181/catalog"                 # DLT_ICEBERG_CATALOG__ICEBERG_CATALOG_CONFIG__URI
type = "rest"                                         # DLT_ICEBERG_CATALOG__ICEBERG_CATALOG_CONFIG__TYPE
warehouse = "default"                                 # DLT_ICEBERG_CATALOG__ICEBERG_CATALOG_CONFIG__WAREHOUSE
header.X-Iceberg-Access-Delegation = "remote-signing" # DLT_ICEBERG_CATALOG__ICEBERG_CATALOG_CONFIG__HEADER_X_ICEBERG_ACCESS_DELEGATION
py-io-impl = "pyiceberg.io.fsspec.FsspecFileIO"       # DLT_ICEBERG_CATALOG__ICEBERG_CATALOG_CONFIG__PY_IO_IMPL
s3.endpoint = "https://awesome-s3-buckets.example"    # DLT_ICEBERG_CATALOG__ICEBERG_CATALOG_CONFIG__S3_ENDPOINT
s3.access-key-id = "x1"                               # DLT_ICEBERG_CATALOG__ICEBERG_CATALOG_CONFIG__S3_ACCESS_KEY_ID
s3.secret-access-key = "y1"                           # DLT_ICEBERG_CATALOG__ICEBERG_CATALOG_CONFIG__S3_SECRET_ACCESS_KEY
s3.region = "eu-fr-1"                                 # DLT_ICEBERG_CATALOG__ICEBERG_CATALOG_CONFIG__S3_REGION
```

That's it!

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

## Partitioning

Apache Iceberg supports [table partitioning](https://iceberg.apache.org/docs/latest/partitioning/) to optimize query performance. There are two ways to configure partitioning:

1. Using the [`iceberg_adapter`](#using-the-iceberg_adapter) function - for advanced partitioning with transformations (year, month, day, hour, bucket, truncate)
2. Using column-level [`partition`](#using-column-level-partition-property) property - for simple identity partitioning

:::note
Iceberg uses [hidden partioning](https://iceberg.apache.org/docs/latest/partitioning/).
:::

:::warning
Partition evolution (changing partition columns after a table has been created) is not supported.
:::

### Using the `iceberg_adapter`

The `iceberg_adapter` function allows you to configure partitioning with various transformation functions.

#### Basic example

```py
from datetime import date

import dlt
from dlt.destinations.adapters import iceberg_adapter, iceberg_partition

data_items = [
    {"id": 1, "category": "A", "created_at": date(2025, 1, 1)},
    {"id": 2, "category": "A", "created_at": date(2025, 1, 15)},
    {"id": 3, "category": "B", "created_at": date(2025, 2, 1)},
]

@dlt.resource(table_format="iceberg")
def events():
    yield data_items

# Partition by category and month of created_at
iceberg_adapter(
    events,
    partition=[
        "category",  # identity partition (shorthand)
        iceberg_partition.month("created_at"),
    ],
)

pipeline = dlt.pipeline("iceberg_example", destination="filesystem")
pipeline.run(events)
```

To use advanced partitioning, import both the adapter and the `iceberg_partition` helper:

```py
from dlt.destinations.adapters import iceberg_adapter, iceberg_partition
```

#### Partition transformations

Iceberg supports several transformation functions for partitioning. Use the `iceberg_partition` helper to create partition specifications:

* `iceberg_partition.identity(column_name)`: Partition by exact column values (this is the same as passing the column name as a string to the `iceberg_adapter`)
* `iceberg_partition.year(column_name)`: Partition by year from a date column
* `iceberg_partition.month(column_name)`: Partition by month from a date column
* `iceberg_partition.day(column_name)`: Partition by day from a date column
* `iceberg_partition.hour(column_name)`: Partition by hour from a timestamp column
* `iceberg_partition.bucket(n, column_name)`: Partition by hashed value into `n` buckets
* `iceberg_partition.truncate(length, column_name)`: Partition by truncated string value to `length`

#### Bucket partitioning

Distribute data across a fixed number of buckets using a hash function:

```py
iceberg_adapter(
    resource,
    partition=[iceberg_partition.bucket(16, "user_id")],
)
```

#### Truncate partitioning

Partition string values by a fixed prefix length:

```py
iceberg_adapter(
    resource,
    partition=[iceberg_partition.truncate(3, "category")],  # "ELECTRONICS" → "ELE"
)
```

#### Custom partition field names

Specify custom names for partition fields:

```py
iceberg_adapter(
    resource,
    partition=[
        iceberg_partition.year("activity_time", "activity_year"),
        iceberg_partition.bucket(8, "user_id", "user_bucket"),
    ],
)
```

### Using column-level `partition` property

For simple identity partitioning, you can use the `partition` column hint directly in the resource definition:

```py
@dlt.resource(
    table_format="iceberg",
    columns={"region": {"partition": True}}
)
def my_iceberg_resource():
    yield [
        {"id": 1, "region": "US", "amount": 100},
        {"id": 2, "region": "EU", "amount": 200},
    ]
```

Multiple columns can be partitioned:

```py
@dlt.resource(
    table_format="iceberg",
    columns={
        "region": {"partition": True},
        "category": {"partition": True},
    }
)
def multi_partition_data():
    ...
```


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
The [`upsert`](../../general-usage/merge-loading.md#upsert-strategy) merge strategy is supported for `iceberg`. This strategy requires that the input data contains no duplicate rows based on the key columns, and that the target table also does not contain duplicates on those keys.

:::warning
Until _pyiceberg_ > 0.9.1 is released, upsert is executed in chunks of **1000** rows.
:::

:::warning
Schema evolution (changing the set of columns) is not supported when using the `upsert` merge strategy with _pyiceberg_ == 0.10.0
:::

```py
@dlt.resource(
    write_disposition={"disposition": "merge", "strategy": "upsert"},
    primary_key="my_primary_key",
    table_format="iceberg"
)
def my_upsert_resource():
    ...
```
