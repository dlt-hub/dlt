---
title: Native Ibis integration
description: Accessing your data with native Ibis backends
keywords: [data, dataset, ibis]
---

# Ibis

Ibis is a powerful portable Python dataframe library. Learn more about what it is and how to use it in the [official documentation](https://ibis-project.org/).

`dlt` provides an easy way to hand over your loaded dataset to an Ibis backend connection.

:::tip
Not all destinations supported by `dlt` have an equivalent Ibis backend. Natively supported destinations include DuckDB (including Motherduck), Postgres, Redshift, Snowflake, Clickhouse, MSSQL (including Synapse), and BigQuery. The filesystem destination is supported via the [Filesystem SQL client](./sql-client#the-filesystem-sql-client); please install the DuckDB backend for Ibis to use it. Mutating data with Ibis on the filesystem will not result in any actual changes to the persisted files.
:::

## Prerequisites

To use the Ibis backend, you will need to have the `ibis-framework` package with the correct Ibis extra installed. The following example will install the DuckDB backend:

```sh
pip install ibis-framework[duckdb]
```

## Get an Ibis connection from your dataset

`dlt` datasets have a helper method to return an Ibis connection to the destination they live on. The returned object is a native Ibis connection to the destination, which you can use to read and even transform data. Please consult the [Ibis documentation](https://ibis-project.org/docs/backends/) to learn more about what you can do with Ibis.

```py
# get the dataset from the pipeline
dataset = pipeline.dataset()
dataset_name = pipeline.dataset_name

# get the native ibis connection from the dataset
ibis_connection = dataset.ibis()

# list all tables in the dataset
# NOTE: You need to provide the dataset name to ibis, in ibis datasets are named databases
print(ibis_connection.list_tables(database=dataset_name))

# get the items table
table = ibis_connection.table("items", database=dataset_name)

# print the first 10 rows
print(table.limit(10).execute())

# Visit the ibis docs to learn more about the available methods
```
