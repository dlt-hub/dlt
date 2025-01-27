---
title: DuckDB
description: DuckDB `dlt` destination
keywords: [duckdb, destination, data warehouse]
---

# DuckDB

## Install dlt with DuckDB
**To install the dlt library with DuckDB dependencies, run:**
```sh
pip install "dlt[duckdb]"
```

## Setup Guide

**1. Initialize a project with a pipeline that loads to DuckDB by running:**
```sh
dlt init chess duckdb
```

**2. Install the necessary dependencies for DuckDB by running:**
```sh
pip install -r requirements.txt
```

**3. Run the pipeline:**
```sh
python3 chess_pipeline.py
```

## Write disposition
All write dispositions are supported.

## Data loading
`dlt` will load data using large INSERT VALUES statements by default. Loading is multithreaded (20 threads by default). If you are okay with installing `pyarrow`, we suggest switching to `parquet` as the file format. Loading is faster (and also multithreaded).

### Names normalization
`dlt` uses the standard **snake_case** naming convention to keep identical table and column identifiers across all destinations. If you want to use the **duckdb** wide range of characters (i.e., emojis) for table and column names, you can switch to the **duck_case** naming convention, which accepts almost any string as an identifier:
* `\n` `\r`  and `"` are translated to `_`
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
**duckdb** identifiers are **case insensitive** but display names preserve case. This may create name collisions if, for example, you load JSON with
`{"Column": 1, "column": 2}` as it will map data to a single column.
:::


## Supported file formats
You can configure the following file formats to load data to duckdb:
* [insert-values](../file-formats/insert-format.md) is used by default
* [parquet](../file-formats/parquet.md) is supported
:::note
`duckdb` cannot COPY many parquet files to a single table from multiple threads. In this situation, `dlt` serializes the loads. Still, that may be faster than INSERT.
:::
* [jsonl](../file-formats/jsonl.md)

:::tip
`duckdb` has [timestamp types](https://duckdb.org/docs/sql/data_types/timestamp.html) with resolutions from milliseconds to nanoseconds. However
only microseconds resolution (the most common used) is time zone aware. `dlt` generates timestamps with timezones by default so loading parquet files
with default settings will fail (`duckdb` does not coerce tz-aware timestamps to naive timestamps).
Disable the timezones by changing `dlt` [parquet writer settings](../file-formats/parquet.md#writer-settings) as follows:
```sh
DATA_WRITER__TIMESTAMP_TIMEZONE=""
```
to disable tz adjustments.
:::

## Supported column hints
`duckdb` may create unique indexes for all columns with `unique` hints, but this behavior **is disabled by default** because it slows the loading down significantly.

## Destination Configuration

By default, a DuckDB database will be created in the current working directory with a name `<pipeline_name>.duckdb` (`chess.duckdb` in the example above). After loading, it is available in `read/write` mode via `with pipeline.sql_client() as con:`, which is a wrapper over `DuckDBPyConnection`. See [duckdb docs](https://duckdb.org/docs/api/python/overview#persistent-storage) for details.

The `duckdb` credentials do not require any secret values. [You are free to pass the credentials and configuration explicitly](../../general-usage/destination.md#pass-explicit-credentials). For example:
```py
# will load data to files/data.db (relative path) database file
p = dlt.pipeline(
  pipeline_name='chess',
  destination=dlt.destinations.duckdb("files/data.db"),
  dataset_name='chess_data',
  dev_mode=False
)

# will load data to /var/local/database.duckdb (absolute path)
p = dlt.pipeline(
  pipeline_name='chess',
  destination=dlt.destinations.duckdb("/var/local/database.duckdb"),
  dataset_name='chess_data',
  dev_mode=False
)
```

The destination accepts a `duckdb` connection instance via `credentials`, so you can also open a database connection yourself and pass it to `dlt` to use.

```py
import duckdb

db = duckdb.connect()
p = dlt.pipeline(
  pipeline_name="chess",
  destination=dlt.destinations.duckdb(db),
  dataset_name="chess_data",
  dev_mode=False,
)

# Or if you would like to use in-memory duckdb instance
db = duckdb.connect(":memory:")
p = pipeline_one = dlt.pipeline(
  pipeline_name="in_memory_pipeline",
  destination=dlt.destinations.duckdb(db),
  dataset_name="chess_data",
)

print(db.sql("DESCRIBE;"))

# Example output
# ┌──────────┬───────────────┬─────────────────────┬──────────────────────┬───────────────────────┬───────────┐
# │ database │    schema     │        name         │     column_names     │     column_types      │ temporary │
# │ varchar  │    varchar    │       varchar       │      varchar[]       │       varchar[]       │  boolean  │
# ├──────────┼───────────────┼─────────────────────┼──────────────────────┼───────────────────────┼───────────┤
# │ memory   │ chess_data    │ _dlt_loads          │ [load_id, schema_n…  │ [VARCHAR, VARCHAR, …  │ false     │
# │ memory   │ chess_data    │ _dlt_pipeline_state │ [version, engine_v…  │ [BIGINT, BIGINT, VA…  │ false     │
# │ memory   │ chess_data    │ _dlt_version        │ [version, engine_v…  │ [BIGINT, BIGINT, TI…  │ false     │
# │ memory   │ chess_data    │ my_table            │ [a, _dlt_load_id, …  │ [BIGINT, VARCHAR, V…  │ false     │
# └──────────┴───────────────┴─────────────────────┴──────────────────────┴───────────────────────┴───────────┘
```

:::note
Be careful! The in-memory instance of the database will be destroyed, once your Python script exits.
:::

This destination accepts database connection strings in the format used by [duckdb-engine](https://github.com/Mause/duckdb_engine#configuration).

You can configure a DuckDB destination with [secret / config values](../../general-usage/credentials) (e.g., using a `secrets.toml` file)
```toml
destination.duckdb.credentials="duckdb:///_storage/test_quack.duckdb"
```

The **duckdb://** URL above creates a **relative** path to `_storage/test_quack.duckdb`. To define an **absolute** path, you need to specify four slashes, i.e., `duckdb:////_storage/test_quack.duckdb`.

Dlt supports a unique connection string that triggers specific behavior for duckdb destination:
* **:pipeline:** creates the database in the working directory of the pipeline, naming it `quack.duckdb`.

Please see the code snippets below showing how to use it

1. Via `config.toml`
```toml
destination.duckdb.credentials=":pipeline:"
```

2. In Python code
```py
p = pipeline_one = dlt.pipeline(
  pipeline_name="my_pipeline",
  destination=dlt.destinations.duckdb(":pipeline:"),
)
```

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

## Additional Setup guides
- [Load data from Slack to DuckDB in python with dlt](https://dlthub.com/docs/pipelines/slack/load-data-with-python-from-slack-to-duckdb)
- [Load data from Airtable to DuckDB in python with dlt](https://dlthub.com/docs/pipelines/airtable/load-data-with-python-from-airtable-to-duckdb)
- [Load data from MongoDB to DuckDB in python with dlt](https://dlthub.com/docs/pipelines/mongodb/load-data-with-python-from-mongodb-to-duckdb)
- [Load data from Sentry to DuckDB in python with dlt](https://dlthub.com/docs/pipelines/sentry/load-data-with-python-from-sentry-to-duckdb)
- [Load data from Cisco Meraki to DuckDB in python with dlt](https://dlthub.com/docs/pipelines/cisco_meraki/load-data-with-python-from-cisco_meraki-to-duckdb)
- [Load data from Pipedrive to DuckDB in python with dlt](https://dlthub.com/docs/pipelines/pipedrive/load-data-with-python-from-pipedrive-to-duckdb)
- [Load data from Notion to DuckDB in python with dlt](https://dlthub.com/docs/pipelines/notion/load-data-with-python-from-notion-to-duckdb)
- [Load data from Chess.com to DuckDB in python with dlt](https://dlthub.com/docs/pipelines/chess/load-data-with-python-from-chess-to-duckdb)
- [Load data from Soundcloud to DuckDB in python with dlt](https://dlthub.com/docs/pipelines/soundcloud/load-data-with-python-from-soundcloud-to-duckdb)
- [Load data from Google Cloud Storage to DuckDB in python with dlt](https://dlthub.com/docs/pipelines/filesystem-gcs/load-data-with-python-from-filesystem-gcs-to-duckdb)
