---
title: DuckDB
description: DuckDB `dlt` destination
keywords: [duckdb, destination, data warehouse]
---

# DuckDB

## Install dlt with DuckDB
**Install the dlt library with DuckDB dependencies:**
```sh
pip install "dlt[duckdb]"
```

<!--@@@DLT_DESTINATION_CAPABILITIES duckdb-->

## Setup guide

**1. Initialize a project with a pipeline that loads to DuckDB:**
```sh
dlt init chess duckdb
```

**2. Install the dependencies for DuckDB:**
```sh
pip install -r requirements.txt
```

**3. Run the pipeline:**
```sh
python3 chess_pipeline.py
```

## Supported versions
`dlt` supports `duckdb` version **0.9** and later. Below are a few notes on problems with particular versions observed
in our tests:
* `1.2.0` and `1.3.2` are verified stable versions where tests consistently pass
* `iceberg_scan` does not work on `duckdb` versions above 1.2.1 and below 1.3.3 with azure blob storage (certain functions are not implemented)
* do not use `1.3.0`. This version has a decimal problem and it segfaults on Windows. Some azure blob storage tests also crash.
* [segfault on windows will be fixed in 1.4](https://github.com/duckdb/duckdb/issues/17971)

## Write disposition
The duckdb destination supports all write dispositions.

## Data loading
By default, `dlt` loads data with large `INSERT VALUES` statements, on 20 threads. Parquet is faster than `INSERT VALUES` and also loads on 20 threads. Parquet needs the `pyarrow` package.

### Data types
`duckdb` supports various [timestamp types](https://duckdb.org/docs/sql/data_types/timestamp.html). You set these types with the column flags `timezone` and `precision`, in the `dlt.resource` decorator or the `pipeline.run` method.

- **Precision**: Supported precision values are 0, 3, 6, and 9 for fractional seconds. You cannot use `timezone` and `precision` together. `dlt` raises an error for that combination.
- **Timezone**:
  - `timezone=False` maps to `TIMESTAMP`.
  - `timezone=True` (or the flag omitted, which defaults to `True`) maps to `TIMESTAMP WITH TIME ZONE` (`TIMESTAMPTZ`).

#### Example precision: TIMESTAMP_MS

```py
@dlt.resource(
    columns={"event_tstamp": {"data_type": "timestamp", "precision": 3}},
    primary_key="event_id",
)
def events():
    yield [{"event_id": 1, "event_tstamp": "2024-07-30T10:00:00.123"}]

pipeline = dlt.pipeline(destination="duckdb")
pipeline.run(events())
```

#### Example timezone: TIMESTAMP

```py
@dlt.resource(
    columns={"event_tstamp": {"data_type": "timestamp", "timezone": False}},
    primary_key="event_id",
)
def events():
    yield [{"event_id": 1, "event_tstamp": "2024-07-30T10:00:00.123+00:00"}]

pipeline = dlt.pipeline(destination="duckdb")
pipeline.run(events())
```

### Name normalization
`dlt` uses the standard **snake_case** naming convention to keep identical table and column identifiers across all destinations. **duckdb** accepts a wide range of characters in table and column names, for example emojis. To use them, switch to the **duck_case** naming convention, which accepts almost any string as an identifier:
* The **duck_case** convention translates new line (`\n`), carriage return (`\r`), and double quotes (`"`) to an underscore (`_`).
* The convention also translates consecutive underscores to a single `_`.

Switch the naming convention in `config.toml`:
```toml
[schema]
naming="duck_case"
```

Or set the env variable `SCHEMA__NAMING`, or set the value in code:
```py
dlt.config["schema.naming"] = "duck_case"
```
:::warning
**duckdb** identifiers are **case-insensitive**, but display names preserve case. If you load JSON with
`{"Column": 1, "column": 2}`, duckdb maps both keys to a single column. This creates a name collision.
:::


## Supported file formats
You can configure the following file formats to load data into duckdb:
* [insert-values](../file-formats.md#sql-insert) is used by default.
* [Parquet](../file-formats.md#parquet) is supported.
:::note
`duckdb` cannot COPY many Parquet files to a single table from multiple threads. In this situation, dlt serializes the loads. Serialized Parquet loads can still be faster than `INSERT VALUES`.
:::
* [JSONL](../file-formats.md#jsonl)

:::tip
`duckdb` has [timestamp types](https://duckdb.org/docs/sql/data_types/timestamp.html) with resolutions from milliseconds to nanoseconds. However,
only the microseconds resolution (the most commonly used) is time-zone-aware. By default, `dlt` generates timestamps with a time zone.
Parquet loads then fail, because `duckdb` does not coerce time-zone-aware timestamps to naive timestamps.
To disable the time zones, change the `dlt` [Parquet writer settings](../file-formats.md#writer-settings):
```sh
DATA_WRITER__TIMESTAMP_TIMEZONE=""
```
Timestamps written this way carry no offset, so `duckdb` reads them in the
[session timezone](#session-timezone), which `dlt` keeps at UTC.
:::

## Supported column hints

`duckdb` can create unique indexes for columns with `unique` hints. However, `dlt` disables this feature by default, because unique indexes make loading much slower.

## Destination config

By default, `dlt` creates a DuckDB database in the current working directory. The name is `<pipeline_name>.duckdb` (`chess.duckdb` in the example above).

The `duckdb` credentials do not require any secret values. [You can pass the credentials and config explicitly](../../general-usage/destination.md#pass-explicit-credentials). For example:
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
Named `duckdb` destinations create a database file in the current working directory as `<destination_name>.duckdb`. For example:
```py
# will load data to files/data.db (relative path) database file
p = dlt.pipeline(
  pipeline_name='chess',
  destination=dlt.destinations.duckdb(destination_name="chessdb"),
  dataset_name='chess_data',
)
```
This code creates the database `chessdb.duckdb`.

:::warning
Do not give the dataset the same name as the database. The `duckdb` binder cannot tell the catalog and the schema apart. For
example:
```py
pipeline = dlt.pipeline(
        pipeline_name="dummy",
        destination="duckdb",
        dataset_name="dummy",
    )
```
This code creates the database `dummy.duckdb` and the schema (dataset) `dummy`. `duckdb` cannot tell them apart and raises a Binder Error.
:::

The destination accepts a `duckdb` connection instance via `credentials`. You can open the database connection yourself and pass it to `dlt`.

```py
import duckdb

db = duckdb.connect()
p = dlt.pipeline(
  pipeline_name="chess",
  destination=dlt.destinations.duckdb(db),
  dataset_name="chess_data",
  dev_mode=False,
)

# Or if you would like to use an in-memory duckdb instance
db = duckdb.connect(":memory:")
p = pipeline_one = dlt.pipeline(
  pipeline_name="in_memory_pipeline",
  destination=dlt.destinations.duckdb(db),
  dataset_name="chess_data",
)

# print(p.run(chess()))

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
When your Python script exits, `duckdb` destroys the in-memory database and all its data.
:::

:::note
`dlt` configures the connection you pass like the ones it opens itself: it sets `search_path` to the
dataset and applies the settings from [Additional config](#additional-config) below.
:::

This destination accepts database connection strings in the format used by [duckdb-engine](https://github.com/Mause/duckdb_engine#configuration).

You can configure a DuckDB destination with [secret / config values](../../general-usage/credentials), for example in a `secrets.toml` file:
```toml
destination.duckdb.credentials="duckdb:///_storage/test_quack.duckdb"
```
The **duckdb://** URL above creates a **relative** path to `_storage/test_quack.duckdb`. To define an **absolute** path, use four slashes: `duckdb:////_storage/test_quack.duckdb`.

You can also skip the schema and pass the path directly:
```toml
destination.duckdb.credentials="_storage/test_quack.duckdb"
```

To place the database in the working directory of the pipeline, pass **:pipeline:** as the path. The name is
`<pipeline_name>.duckdb`, or `<destination_name>.duckdb` for a named destination.

1. Via `config.toml`
```toml
destination.duckdb.credentials=":pipeline:"
```

2. In Python code
```py
p = dlt.pipeline(
  pipeline_name="my_pipeline",
  destination=dlt.destinations.duckdb(":pipeline:"),
)
```

### Additional config
If you set the following config value, `dlt` creates unique indexes during loading:
```toml
[destination.duckdb]
create_indexes=true
```

Add extensions, pragmas, and [local and global config](https://duckdb.org/docs/stable/configuration/overview.html#global-configuration-options) options to the config:
```toml
[destination.duckdb.credentials]
extensions=["spatial"]
pragmas=["enable_progress_bar", "enable_logging"]

[destination.duckdb.credentials.global_config]
azure_transport_option_type=true

[destination.duckdb.credentials.local_config]
errors_as_json=true
```
The config above runs these steps in order:
* `LOAD spatial` — `dlt` loads the extension but does not install it.
* the global config: `SET GLOBAL azure_transport_option_type=true`
* the `statements`, if you set any
* the pragmas: `PRAGMA enable_logging`
* the local config: `SET SESSION errors_as_json=true`

Internally, `dlt` opens a new `duckdb` connection and then dispenses separate sessions to worker threads with `cursor()`. `dlt` does this even when the calling thread is the thread that created the connection. `dlt` applies extensions and global config once, on the "original" connection, and `duckdb` propagates them to every session.

#### Session timezone
`dlt` sets `TimeZone` to `UTC` on every connection it opens, and each session it dispenses inherits
it. This setting decides how `duckdb` reads values that arrive without a UTC offset, for example
naive timestamps in Parquet. It also decides which timezone `duckdb` returns for `TIMESTAMPTZ`
columns. It does not change the column types that `CREATE TABLE` produces.

Set `session_timezone` to a different timezone. To keep the `duckdb` default, which is the machine
timezone, set it to an empty string:
```toml
[destination.duckdb.credentials]
session_timezone="Europe/Berlin"
```

`TimeZone` is a database-wide setting, so on a connection that you passed yourself it becomes the
default for the sessions you open from it as well. A session that set its own `TimeZone` keeps it.

#### SQL statements on each connection
The `statements` option takes plain SQL for anything the options above cannot express, such as `INSTALL`, `ATTACH` and `CREATE SECRET`:
```toml
[destination.duckdb.credentials]
statements=["INSTALL lance", "LOAD lance"]
```
The example pairs `INSTALL` with `LOAD` to keep both steps together. You can also load the extension with
the `extensions` option instead.

`dlt` runs these statements on the "original" connection, right after the extensions and global config.
Like those two options, the statements run once and propagate to every session.

This option is for database-scoped statements only. `INSTALL`, `LOAD`, `ATTACH`, `CREATE SECRET`,
`CREATE SCHEMA` and `CREATE VIEW` all belong to the database. `SET SESSION` and `USE` apply only to the
"original" connection. They do not propagate to the sessions, and `dlt` reports no error. Put session
settings in `pragmas` and `local_config` instead.

Such statements often carry credentials, so `dlt` treats `statements` as a secret. Put the value in
`secrets.toml`, not `config.toml`.

You can pass dictionaries and lists in environment variables. Write these values as Python literals, not as JSON.

You can also pass additional options in code:
```py
import os
import duckdb
from dlt.destinations.impl.duckdb.configuration import DuckDbCredentials

# install spatial
duckdb.sql("INSTALL spatial;")

# use Python list literal to pass complex env variable
os.environ["DESTINATION__DUCKDB__CREDENTIALS__PRAGMAS"] = '["enable_logging"]'

dest_ = dlt.destinations.duckdb(
    DuckDbCredentials("duck.db", extensions=["spatial"], local_config={"errors_as_json": True})
)
```
The code above installs **spatial** with `duckdb` directly, because `dlt` only loads an extension. The code then passes duckdb credentials to the destination constructor. The database file is **duck.db**, and the code enables logging and `json` error messages.

## Data access after loading
After a load, you can read and write the data with `with pipeline.sql_client() as con:`. This client wraps `DuckDBPyConnection`. See [duckdb docs](https://duckdb.org/docs/api/python/overview#persistent-storage) for details. If you want to **read** data, use [pipeline.dataset()](../../general-usage/dataset-access/dataset) instead of `sql_client`.


## dbt support
This destination [integrates with dbt](../transformations/dbt/dbt.md) via [dbt-duckdb](https://github.com/jwills/dbt-duckdb), which is a community-supported package. `dlt` shares the `duckdb` database with `dbt`. In rare cases, `dbt-duckdb` reports that the binary database format does not match the format it expects. To avoid this error, update the `duckdb` package in your `dlt` project with `pip install -U`.

:::note
`dlt` does not propagate extensions, pragmas, and config options to the dbt profile.
:::

### Syncing of `dlt` state
This destination fully supports [dlt state sync](../../general-usage/state#syncing-state-with-destination).

<!--@@@DLT_TUBA duckdb-->

