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

:::warning
While ephemeral catalogs make it easy to get started with Iceberg, it comes with limitations:
- concurrent writes are not handled and may lead to corrupt table state
- we cannot guarantee that reads concurrent with writes are clean
- the latest manifest file needs to be searched for using file listing—this can become slow with large tables, especially in cloud object stores
:::

:::tip dltHub Features
If you're interested in a multi-user cloud experience and integration with vendor catalogs, such as Polaris or Unity Catalog, check out [dltHub Iceberg destination](https://info.dlthub.com/waiting-list).
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
