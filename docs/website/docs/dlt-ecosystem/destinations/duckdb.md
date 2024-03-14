---
title: DuckDB
description: DuckDB `dlt` destination
keywords: [duckdb, destination, data warehouse]
---

# DuckDB

## Install dlt with DuckDB
**To install the DLT library with DuckDB dependencies, run:**
```
pip install dlt[duckdb]
```

## Setup Guide

**1. Initialize a project with a pipeline that loads to DuckDB by running:**
```
dlt init chess duckdb
```

**2. Install the necessary dependencies for DuckDB by running:**
```
pip install -r requirements.txt
```

**3. Run the pipeline:**
```
python3 chess_pipeline.py
```

## Write disposition
All write dispositions are supported.

## Data loading
`dlt` will load data using large INSERT VALUES statements by default. Loading is multithreaded (20 threads by default). If you are okay with installing `pyarrow`, we suggest switching to `parquet` as the file format. Loading is faster (and also multithreaded).

### Names normalization
`dlt` uses the standard **snake_case** naming convention to keep identical table and column identifiers across all destinations. If you want to use the **duckdb** wide range of characters (i.e., emojis) for table and column names, you can switch to the **duck_case** naming convention, which accepts almost any string as an identifier:
* `\n` `\r`  and `" are translated to `_`
* multiple `_` are translated to a single `_`

Switch the naming convention using `config.toml`:
```toml
[schema]
naming="duck_case"
```

or via the env variable `SCHEMA__NAMING` or directly in the code:
```py
dlt.config["schema.naming"] = "duck_case"
```
:::caution
**duckdb** identifiers are **case insensitive** but display names preserve case. This may create name clashes if, for example, you load JSON with
`{"Column": 1, "column": 2}` as it will map data to a single column.
:::


## Supported file formats
You can configure the following file formats to load data to duckdb:
* [insert-values](../file-formats/insert-format.md) is used by default
* [parquet](../file-formats/parquet.md) is supported
:::note
`duckdb` cannot COPY many parquet files to a single table from multiple threads. In this situation, `dlt` serializes the loads. Still, that may be faster than INSERT.
:::
* [jsonl](../file-formats/jsonl.md) **is supported but does not work if JSON fields are optional. The missing keys fail the COPY instead of being interpreted as NULL.**

## Supported column hints
`duckdb` may create unique indexes for all columns with `unique` hints, but this behavior **is disabled by default** because it slows the loading down significantly.

## Destination Configuration

By default, a DuckDB database will be created in the current working directory with a name `<pipeline_name>.duckdb` (`chess.duckdb` in the example above). After loading, it is available in `read/write` mode via `with pipeline.sql_client() as con:`, which is a wrapper over `DuckDBPyConnection`. See [duckdb docs](https://duckdb.org/docs/api/python/overview#persistent-storage) for details.

The `duckdb` credentials do not require any secret values. You are free to pass the configuration explicitly via the `credentials` parameter to `dlt.pipeline` or `pipeline.run` methods. For example:
```py
# will load data to files/data.db database file
p = dlt.pipeline(pipeline_name='chess', destination='duckdb', dataset_name='chess_data', full_refresh=False, credentials="files/data.db")

# will load data to /var/local/database.duckdb
p = dlt.pipeline(pipeline_name='chess', destination='duckdb', dataset_name='chess_data', full_refresh=False, credentials="/var/local/database.duckdb")
```

The destination accepts a `duckdb` connection instance via `credentials`, so you can also open a database connection yourself and pass it to `dlt` to use. `:memory:` databases are supported.
```py
import duckdb
db = duckdb.connect()
p = dlt.pipeline(pipeline_name='chess', destination='duckdb', dataset_name='chess_data', full_refresh=False, credentials=db)
```

This destination accepts database connection strings in the format used by [duckdb-engine](https://github.com/Mause/duckdb_engine#configuration).

You can configure a DuckDB destination with [secret / config values](../../general-usage/credentials) (e.g., using a `secrets.toml` file)
```toml
destination.duckdb.credentials=duckdb:///_storage/test_quack.duckdb
```
The **duckdb://** URL above creates a **relative** path to `_storage/test_quack.duckdb`. To define an **absolute** path, you need to specify four slashes, i.e., `duckdb:////_storage/test_quack.duckdb`.

A few special connection strings are supported:
* **:pipeline:** creates the database in the working directory of the pipeline with the name `quack.duckdb`.
* **:memory:** creates an in-memory database. This may be useful for testing.


### Additional configuration
Unique indexes may be created during loading if the following config value is set:
```toml
[destination.duckdb]
create_indexes=true
```

### dbt support
This destination [integrates with dbt](../transformations/dbt/dbt.md) via [dbt-duckdb](https://github.com/jwills/dbt-duckdb), which is a community-supported package. The `duckdb` database is shared with `dbt`. In rare cases, you may see information that the binary database format does not match the database format expected by `dbt-duckdb`. You can avoid that by updating the `duckdb` package in your `dlt` project with `pip install -U`.

### Syncing of `dlt` state
This destination fully supports [dlt state sync](../../general-usage/state#syncing-state-with-destination).

<!--@@@DLT_SNIPPET_START tuba::duckdb-->
## Additional Setup guides

- [Load data from Google Analytics to DuckDB in python with dlt](https://dlthub.com/docs/pipelines/google_analytics/load-data-with-python-from-google_analytics-to-duckdb)
- [Load data from Google Sheets to DuckDB in python with dlt](https://dlthub.com/docs/pipelines/google_sheets/load-data-with-python-from-google_sheets-to-duckdb)
- [Load data from Stripe to DuckDB in python with dlt](https://dlthub.com/docs/pipelines/stripe_analytics/load-data-with-python-from-stripe_analytics-to-duckdb)
- [Load data from Notion to DuckDB in python with dlt](https://dlthub.com/docs/pipelines/notion/load-data-with-python-from-notion-to-duckdb)
- [Load data from Chess.com to DuckDB in python with dlt](https://dlthub.com/docs/pipelines/chess/load-data-with-python-from-chess-to-duckdb)
- [Load data from HubSpot to DuckDB in python with dlt](https://dlthub.com/docs/pipelines/hubspot/load-data-with-python-from-hubspot-to-duckdb)
- [Load data from GitHub to DuckDB in python with dlt](https://dlthub.com/docs/pipelines/github/load-data-with-python-from-github-to-duckdb)
<!--@@@DLT_SNIPPET_END tuba::duckdb-->
