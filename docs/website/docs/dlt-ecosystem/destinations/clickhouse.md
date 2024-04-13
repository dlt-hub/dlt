---
title: ClickHouse
description: ClickHouse `dlt` destination
keywords: [ clickhouse, destination, data warehouse ]
---

# ClickHouse

## Install dlt with ClickHouse

**To install the DLT library with ClickHouse dependencies:**

```sh
pip install dlt[clickhouse]
```

## Setup Guide

### 1. Initialize the dlt project

Let's start by initializing a new `dlt` project as follows:

```sh
dlt init chess clickhouse
```

> 💡 This command will initialize your pipeline with chess as the source and ClickHouse as the destination.

The above command generates several files and directories, including `.dlt/secrets.toml` and a requirements file for ClickHouse. You can install the necessary dependencies specified in the
requirements file by executing it as follows:

```sh
pip install -r requirements.txt
```

or with `pip install dlt[clickhouse]`, which installs the `dlt` library and the necessary dependencies for working with ClickHouse as a destination.

### 2. Setup ClickHouse database

To load data into ClickHouse, you need to create a ClickHouse database. While we recommend asking our GPT-4 assistant for details, we have provided a general outline of the process below:

1. You can use an existing ClickHouse database or create a new one.

2. To create a new database, connect to your ClickHouse server using the `clickhouse-client` command line tool or a SQL client of your choice.

3. Run the following SQL commands to create a new database, user and grant the necessary permissions:

   ```sql
   CREATE DATABASE IF NOT EXISTS dlt;
   CREATE USER dlt IDENTIFIED WITH sha256_password BY 'Dlt*12345789234567';
   GRANT CREATE, ALTER, SELECT, DELETE, DROP, TRUNCATE, OPTIMIZE, SHOW, INSERT, dictGet ON dlt.* TO dlt;
   GRANT SELECT ON INFORMATION_SCHEMA.COLUMNS TO dlt;
   GRANT CREATE TEMPORARY TABLE, S3 ON *.* TO dlt;
   ```

### 3. Add credentials

1. Next, set up the ClickHouse credentials in the `.dlt/secrets.toml` file as shown below:

   ```toml
   [destination.clickhouse.credentials]
   database = "dlt"                         # The database name you created
   username = "dlt"                         # ClickHouse username, default is usually "default"
   password = "Dlt*12345789234567"          # ClickHouse password if any
   host = "localhost"                       # ClickHouse server host
   port = 9000                              # ClickHouse HTTP port, default is 9000
   http_port = 8443                         # HTTP Port to connect to ClickHouse server's HTTP interface.
   secure = 1                               # Set to 1 if using HTTPS, else 0.
   dataset_table_separator = "___"          # Separator for dataset table names from dataset.
   ```

2. You can pass a database connection string similar to the one used by the `clickhouse-driver` library. The credentials above will look like this:

   ```toml
   # keep it at the top of your toml file, before any section starts.
   destination.clickhouse.credentials="clickhouse://dlt:Dlt*12345789234567@localhost:9000/dlt?secure=1"
   ```

## Write disposition

All [write dispositions](../../general-usage/incremental-loading#choosing-a-write-disposition) are supported.

## Data loading

Data is loaded into ClickHouse using the most efficient method depending on the data source:

- For local files, the `clickhouse-connect` library is used to directly load files into ClickHouse tables using the `INSERT` command.
- For files in remote storage like S3, Google Cloud Storage, or Azure Blob Storage, ClickHouse table functions like `s3`, `gcs` and `azureBlobStorage` are used to read the files and insert the data
  into tables.

## Supported file formats

- [jsonl](../file-formats/jsonl.md) is the preferred format for both direct loading and staging.
- [parquet](../file-formats/parquet.md) is supported for both direct loading and staging.

## Supported column hints

ClickHouse supports the following [column hints](https://dlthub.com/docs/general-usage/schema#tables-and-columns):

- `primary_key` - marks the column as part of the primary key. Multiple columns can have this hint to create a composite primary key.

## Table Engine

By default, tables are created using the `ReplicatedMergeTree` table engine in ClickHouse. You can specify an alternate table engine using the `table_engine_type` hint on the resource:

```py
@dlt.resource(table_engine_type="merge_tree")
def my_resource():
  ...
```

Supported values are:

- `merge_tree` - creates tables using the `MergeTree` engine
- `replicated_merge_tree` (default) - creates tables using the `ReplicatedMergeTree` engine

## Staging support

ClickHouse supports Amazon S3, Google Cloud Storage and Azure Blob Storage as file staging destinations.

`dlt` will upload Parquet or JSONL files to the staging location and use ClickHouse table functions to load the data directly from the staged files.

Please refer to the filesystem documentation to learn how to configure credentials for the staging destinations:

- [Amazon S3](./filesystem.md#aws-s3)
- [Google Cloud Storage](./filesystem.md#google-storage)
- [Azure Blob Storage](./filesystem.md#azure-blob-storage)

To run a pipeline with staging enabled:

```py
pipeline = dlt.pipeline(
  pipeline_name='chess_pipeline',
  destination='clickhouse',
  staging='filesystem',  # add this to activate staging
  dataset_name='chess_data'
)
```

### dbt support

Integration with [dbt](../transformations/dbt/dbt.md) is supported.

### Syncing of `dlt` state

This destination fully supports [dlt state sync](../../general-usage/state#syncing-state-with-destination).

<!--@@@DLT_TUBA clickhouse-->