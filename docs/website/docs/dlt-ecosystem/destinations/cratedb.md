---
title: CrateDB
description: CrateDB `dlt` destination
keywords: [ cratedb, destination, data warehouse ]
---

# CrateDB

## Install dlt with CrateDB

**To install the DLT library with CrateDB dependencies:**

```sh
pip install "dlt[cratedb]"
```

## Setup guide

### 1. Initialize the dlt project

Let's start by initializing a new `dlt` project as follows:

```sh
dlt init chess cratedb
```

Because CrateDB currently only supports writing to its default `doc` schema with dlt,
please replace `dataset_name="chess_players_games_data"` with `dataset_name="doc"`.

The `dlt init` command will initialize your pipeline with `chess` as the source and
`cratedb` as the destination.

The above command generates several files and directories, including `.dlt/secrets.toml`.

### 2. Configure credentials

Next, set up the CrateDB credentials in the `.dlt/secrets.toml` file as shown below.
CrateDB is compatible with PostgreSQL and uses the `psycopg2` driver, like the
`postgres` destination.

```toml
[destination.cratedb.credentials]
host = "localhost"                       # CrateDB server host.
port = 5432                              # CrateDB PostgreSQL TCP protocol port, default is 5432.
username = "crate"                       # CrateDB username, default is usually "crate".
password = ""                            # CrateDB password, if any.
```

Alternatively, You can pass a database connection string as shown below.
```toml
destination.cratedb.credentials="postgres://crate:@localhost:5432/"
```
Keep it at the top of your TOML file, before any section starts.
Because CrateDB uses `psycopg2`, using `postgres://` is the right choice.

Use Docker or Podman to run an instance of CrateDB for evaluation purposes.
```shell
docker run --rm -it --name=cratedb --publish=4200:4200 --publish=5432:5432 crate:latest -Cdiscovery.type=single-node
```

## Data loading

Data is loaded into CrateDB using the most efficient method depending on the data source:

- For local files, the `psycopg2` library is used to directly load files into
  CrateDB tables using the `INSERT` command.
- For files in remote storage like S3 or Azure Blob Storage,
  CrateDB data loading functions are used to read the files and insert the data into tables.

## Datasets

CrateDB currently only supports working with its default schema `doc`.
So, please use `dataset_name="doc"`.

## Supported file formats

- [INSERT](../file-formats/insert-format.md) is the preferred format for both direct loading and staging.

The `cratedb` destination has a few specific deviations from the default SQL destinations:

- CrateDB does not support the `time` datatype. Time will be loaded to a `text` column.
- CrateDB does not support the `binary` datatype. Binary will be loaded to a `text` column.
- CrateDB can produce rounding errors under certain conditions when using the `float/double` datatype.
  Make sure to use the `decimal` datatype if you can’t afford to have rounding errors.

## Supported column hints

CrateDB supports the following [column hints](../../general-usage/schema#tables-and-columns):

- `primary_key` - marks the column as part of the primary key. Multiple columns can have this hint to create a composite primary key.

## Staging support

CrateDB supports Amazon S3, Google Cloud Storage, and Azure Blob Storage as file staging destinations.

`dlt` will upload CSV or JSONL files to the staging location and use CrateDB data loading functions
to load the data directly from the staged files.

Please refer to the filesystem documentation to learn how to configure credentials for the staging destinations:

- [Amazon S3](./filesystem.md#aws-s3)
- [Azure Blob Storage](./filesystem.md#azure-blob-storage)

To run a pipeline with staging enabled:

```py
pipeline = dlt.pipeline(
  pipeline_name='chess_pipeline',
  destination='cratedb',
  staging='filesystem',  # add this to activate staging
  dataset_name='chess_data'
)
```

### dbt support

Integration with [dbt](../transformations/dbt/dbt.md) is generally supported via [dbt-cratedb2]
but not tested by us.

### Syncing of `dlt` state

This destination fully supports [dlt state sync](../../general-usage/state#syncing-state-with-destination).


[dbt-cratedb2]: https://pypi.org/project/dbt-cratedb2/

<!--@@@DLT_TUBA CrateDB-->
