---
title: DuckDB
description: DuckDB `dlt` destination
keywords: [duckdb, destination, data warehouse]
---

# DuckDB

## Install dlt with DuckDB
**To install the DLT library with DuckDB dependencies:**
```
pip install dlt[duckdb]
```

## Setup Guide

**1. Initialize a project with a pipeline that loads to DuckDB by running**
```
dlt init chess duckdb
```

**2. Install the necessary dependencies for DuckDB by running**
```
pip install -r requirements.txt
```

**3. Run the pipeline**
```
python3 chess_pipeline.py
```

## Write disposition
All write dispositions are supported

## Data loading
`dlt` will load data using large INSERT VALUES statements by default. Loading is multithreaded (20 threads by default). If you are ok with installing `pyarrow` we suggest to switch to `parquet` as file format. Loading is faster (and also multithreaded).

### Names normalization
`dlt` uses standard **snake_case** naming convention to keep identical table and column identifiers across all destinations. If you want to use **duckdb** wide range of characters (ie. emojis) for table and column names, you can switch to **duck_case** naming convention which accepts almost any string as an identifier:
* `\n` `\r`  and `" are translated to `_`
* multiple `_` are translated to single `_`

Switch the naming convention using `config.toml`:
```toml
[schema]
naming="duck_case"
```

or via env variable `SCHEMA__NAMING` or directly in code:
```python
dlt.config["schema.naming"] = "duck_case"
```
:::caution
**duckdb** identifiers are **case insensitive** but display names preserve case. This may create name clashes if for example you load json with
`{"Column": 1, "column": 2}` will map data to a single column.
:::


## Supported file formats
You can configure the following file formats to load data to duckdb
* [insert-values](../file-formats/insert-format.md) is used by default
* [parquet](../file-formats/parquet.md) is supported
:::note
`duckdb` cannot COPY many parquet files to a single table from multiple threads. In this situation `dlt` serializes the loads. Still - that may be faster than INSERT
:::
* [jsonl](../file-formats/jsonl.md) **is supported but does not work if JSON fields are optional. the missing keys fail the COPY instead of being interpreted as NULL**

## Supported column hints
`duckdb` may create unique indexes for all columns with `unique` hints but this behavior **is disabled by default** because it slows the loading down significantly.

## Destination Configuration

By default, a DuckDB database will be created in the current working directory with a name `<pipeline_name>.duckdb` (`chess.duckdb` in the example above). After loading, it is available in `read/write` mode via `with pipeline.sql_client() as con:` which is a wrapper over `DuckDBPyConnection`. See [duckdb docs](https://duckdb.org/docs/api/python/overview#persistent-storage) for details.

The `duckdb` credentials do not require any secret values. You are free to pass the configuration explicitly via the `credentials` parameter to `dlt.pipeline` or `pipeline.run` methods. For example:
```python
# will load data to files/data.db database file
p = dlt.pipeline(pipeline_name='chess', destination='duckdb', dataset_name='chess_data', full_refresh=False, credentials="files/data.db")

# will load data to /var/local/database.duckdb
p = dlt.pipeline(pipeline_name='chess', destination='duckdb', dataset_name='chess_data', full_refresh=False, credentials="/var/local/database.duckdb")
```

The destination accepts a `duckdb` connection instance via `credentials`, so you can also open a database connection yourself and pass it to `dlt` to use. `:memory:` databases are supported.
```python
import duckdb
db = duckdb.connect()
p = dlt.pipeline(pipeline_name='chess', destination='duckdb', dataset_name='chess_data', full_refresh=False, credentials=db)
```

This destination accepts database connection strings in format used by [duckdb-engine](https://github.com/Mause/duckdb_engine#configuration).

You can configure a DuckDB destination with [secret / config values](../../general-usage/credentials) (e.g. using a `secrets.toml` file)
```toml
destination.duckdb.credentials=duckdb:///_storage/test_quack.duckdb
```
**duckdb://** url above creates a **relative** path to `_storage/test_quack.duckdb`. To define **absolute** path you need to specify four slashes ie. `duckdb:////_storage/test_quack.duckdb`.

A few special connection strings are supported:
* **:pipeline:** creates the database in the working directory of the pipeline with name `quack.duckdb`.
* **:memory:** creates in memory database. This may be useful for testing.


### Additional configuration
Unique indexes may be created during loading if the following config value is set:
```toml
[destination.duckdb]
create_indexes=true
```

### dbt support
This destination [integrates with dbt](../transformations/dbt/dbt.md) via [dbt-duckdb](https://github.com/jwills/dbt-duckdb) which is a community supported package. The `duckdb` database is shared with `dbt`. In rare cases you may see information that binary database format does not match the database format expected by `dbt-duckdb`. You may avoid that by updating the `duckdb` package in your `dlt` project with `pip install -U`.

### Syncing of `dlt` state
This destination fully supports [dlt state sync](../../general-usage/state#syncing-state-with-destination)
