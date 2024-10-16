---
title: AWS Athena / Glue Catalog
description: AWS Athena `dlt` destination
keywords: [aws, athena, glue catalog]
---

# AWS Athena / Glue Catalog

The Athena destination stores data as Parquet files in S3 buckets and creates [external tables in AWS Athena](https://docs.aws.amazon.com/athena/latest/ug/creating-tables.html). You can then query those tables with Athena SQL commands, which will scan the entire folder of Parquet files and return the results. This destination works very similarly to other SQL-based destinations, with the exception that the merge write disposition is not supported at this time. The `dlt` metadata will be stored in the same bucket as the Parquet files, but as iceberg tables. Athena also supports writing individual data tables as Iceberg tables, so they may be manipulated later. A common use case would be to strip GDPR data from them.

## Install dlt with Athena
**To install the dlt library with Athena dependencies:**
```sh
pip install "dlt[athena]"
```

## Setup guide
### 1. Initialize the dlt project

Let's start by initializing a new `dlt` project as follows:
   ```sh
   dlt init chess athena
   ```
   > 💡 This command will initialize your pipeline with chess as the source and AWS Athena as the destination using the filesystem staging destination.


### 2. Setup bucket storage and Athena credentials

First, install dependencies by running:
```sh
pip install -r requirements.txt
```
or with `pip install "dlt[athena]"`, which will install `s3fs`, `pyarrow`, `pyathena`, and `botocore` packages.

:::caution

You may also install the dependencies independently. Try
```sh
pip install dlt
pip install s3fs
pip install pyarrow
pip install pyathena
```
so pip does not fail on backtracking.
:::

To edit the `dlt` credentials file with your secret info, open `.dlt/secrets.toml`. You will need to provide a `bucket_url`, which holds the uploaded parquet files, a `query_result_bucket`, which Athena uses to write query results to, and credentials that have write and read access to these two buckets as well as the full Athena access AWS role.

The TOML file looks like this:

```toml
[destination.filesystem]
bucket_url = "s3://[your_bucket_name]" # replace with your bucket name,

[destination.filesystem.credentials]
aws_access_key_id = "please set me up!" # copy the access key here
aws_secret_access_key = "please set me up!" # copy the secret access key here

[destination.athena]
query_result_bucket="s3://[results_bucket_name]" # replace with your query results bucket name

[destination.athena.credentials]
aws_access_key_id="please set me up!" # same as credentials for filesystem
aws_secret_access_key="please set me up!" # same as credentials for filesystem
region_name="please set me up!" # set your AWS region, for example "eu-central-1" for Frankfurt
```

If you have your credentials stored in `~/.aws/credentials`, just remove the **[destination.filesystem.credentials]** and **[destination.athena.credentials]** sections above and `dlt` will fall back to your **default** profile in local credentials. If you want to switch the profile, pass the profile name as follows (here: `dlt-ci-user`):
```toml
[destination.filesystem.credentials]
profile_name="dlt-ci-user"

[destination.athena.credentials]
profile_name="dlt-ci-user"
```

## Additional destination configuration

You can provide an Athena workgroup like so:
```toml
[destination.athena]
athena_work_group="my_workgroup"
```

## Write disposition

The `athena` destination handles the write dispositions as follows:
- `append` - files belonging to such tables are added to the dataset folder.
- `replace` - all files that belong to such tables are deleted from the dataset folder, and then the current set of files is added.
- `merge` - falls back to `append` (unless you're using [iceberg](#iceberg-data-tables) tables).

## Data loading

Data loading occurs by storing parquet files in an S3 bucket and defining a schema on Athena. If you query data via SQL queries on Athena, the returned data is read by scanning your bucket and reading all relevant parquet files in there.

`dlt` internal tables are saved as Iceberg tables.

### Data types
Athena tables store timestamps with millisecond precision, and with that precision, we generate parquet files. Keep in mind that Iceberg tables have microsecond precision.

Athena does not support JSON fields, so JSON is stored as a string.

> ❗**Athena does not support TIME columns in parquet files**. `dlt` will fail such jobs permanently. Convert `datetime.time` objects to `str` or `datetime.datetime` to load them.

### Table and column identifiers

Athena uses case-insensitive identifiers and **will lowercase all the identifiers** that are stored in the INFORMATION SCHEMA. Do not use [case-sensitive naming conventions](../../general-usage/naming-convention.md#case-sensitive-and-insensitive-destinations). Letter casing will be removed anyway, and you risk generating identifier collisions, which are detected by `dlt` and will fail the load process.

Under the hood, Athena uses different SQL engines for DDL (catalog) and DML/Queries:
* DDL uses HIVE escaping with ``````
* Other queries use PRESTO and regular SQL escaping.

## Staging support

Using a staging destination is mandatory when using the Athena destination. If you do not set staging to `filesystem`, `dlt` will automatically do this for you.

If you decide to change the [filename layout](./filesystem#data-loading) from the default value, keep the following in mind so that Athena can reliably build your tables:
 - You need to provide the `{table_name}` placeholder, and this placeholder needs to be followed by a forward slash.
 - You need to provide the `{file_id}` placeholder, and it needs to be somewhere after the `{table_name}` placeholder.
 - `{table_name}` must be the first placeholder in the layout.

## Additional destination options

### Iceberg data tables

You can save your tables as Iceberg tables to Athena. This will enable you, for example, to delete data from them later if you need to. To switch a resource to the Iceberg table format, supply the table_format argument like this:

```py
@dlt.resource(table_format="iceberg")
def data() -> Iterable[TDataItem]:
    ...
```

For every table created as an Iceberg table, the Athena destination will create a regular Athena table in the staging dataset of both the filesystem and the Athena glue catalog, and then copy all data into the final Iceberg table that lives with the non-Iceberg tables in the same dataset on both the filesystem and the glue catalog. Switching from Iceberg to regular table or vice versa is not supported.

#### `merge` support

The `merge` write disposition is supported for Athena when using Iceberg tables.

> Note that:
> 1. There is a risk of tables ending up in an inconsistent state in case a pipeline run fails mid-flight because Athena doesn't support transactions, and `dlt` uses multiple DELETE/UPDATE/INSERT statements to implement `merge`.
> 2. `dlt` creates additional helper tables called `insert_<table name>` and `delete_<table name>` in the staging schema to work around Athena's lack of temporary tables.

### dbt support

Athena is supported via `dbt-athena-community`. Credentials are passed into `aws_access_key_id` and `aws_secret_access_key` of the generated dbt profile. Iceberg tables are supported, but you need to make sure that you materialize your models as Iceberg tables if your source table is Iceberg. We encountered problems with materializing date-time columns due to different precision on Iceberg (nanosecond) and regular Athena tables (millisecond).
The Athena adapter requires that you set up **region_name** in the Athena configuration below. You can also set up the table catalog name to change the default: **awsdatacatalog**
```toml
[destination.athena]
aws_data_catalog="awsdatacatalog"
```

### Syncing of `dlt` state

- This destination fully supports [dlt state sync.](../../general-usage/state#syncing-state-with-destination). The state is saved in Athena Iceberg tables in your S3 bucket.

## Supported file formats

You can choose the following file formats:
* [parquet](../file-formats/parquet.md) is used by default

## Athena adapter

You can use the `athena_adapter` to add partitioning to Athena tables. This is currently only supported for Iceberg tables.

Iceberg tables support a few transformation functions for partitioning. Info on all supported functions in the [AWS documentation](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-creating-tables.html#querying-iceberg-creating-tables-query-editor).

Use the `athena_partition` helper to generate the partitioning hints for these functions:

* `athena_partition.year(column_name: str)`: Partition by year of date/datetime column.
* `athena_partition.month(column_name: str)`: Partition by month of date/datetime column.
* `athena_partition.day(column_name: str)`: Partition by day of date/datetime column.
* `athena_partition.hour(column_name: str)`: Partition by hour of date/datetime column.
* `athena_partition.bucket(n: int, column_name: str)`: Partition by hashed value to `n` buckets
* `athena_partition.truncate(length: int, column_name: str)`: Partition by truncated value to `length` (or width for numbers)

Here is an example of how to use the adapter to partition a table:

```py
from datetime import date

import dlt
from dlt.destinations.adapters import athena_partition, athena_adapter

data_items = [
    (1, "A", date(2021, 1, 1)),
    (2, "A", date(2021, 1, 2)),
    (3, "A", date(2021, 1, 3)),
    (4, "A", date(2021, 2, 1)),
    (5, "A", date(2021, 2, 2)),
    (6, "B", date(2021, 1, 1)),
    (7, "B", date(2021, 1, 2)),
    (8, "B", date(2021, 1, 3)),
    (9, "B", date(2021, 2, 1)),
    (10, "B", date(2021, 3, 2)),
]

@dlt.resource(table_format="iceberg")
def partitioned_data():
    yield [{"id": i, "category": c, "created_at": d} for i, c, d in data_items]

# Add partitioning hints to the table
athena_adapter(
    partitioned_table,
    partition=[
        # Partition per category and month
        "category",
        athena_partition.month("created_at"),
    ],
)


pipeline = dlt.pipeline("athena_example")
pipeline.run(partitioned_data)
```
<!--@@@DLT_TUBA athena-->

