---
title: 🧪 MotherDuck
description: MotherDuck `dlt` destination
keywords: [MotherDuck, duckdb, destination, data warehouse]
---

# MotherDuck

> 🧪 MotherDuck is still invitation only and intensively tested. Please see the limitations / problems at the end.

:::tip
Decrease the number of load workers to 3-5 depending on the quality of your internet connection if you see a lot of retries in your logs with various timeout, add the following to your `config.toml`:
```toml
[load]
workers=3
```
or export **LOAD__WORKERS=3** env variable. See more in [performance](../../reference/performance.md)
:::

## Setup Guide

**1. Initialize a project with a pipeline that loads to MotherDuck by running**
```
dlt init chess motherduck
```

**2. Install the necessary dependencies for MotherDuck by running**
```
pip install -r requirements.txt
```

This will install dlt with **motherduck** extra which contains **duckdb** and **pyarrow** dependencies

**3. Add your MotherDuck token to `.dlt/secrets.toml`**
```toml
[destination.motherduck.credentials]
database = "dlt_data_3"
password = "<your token here>"
```
Paste your **service token** into password. The `database` field is optional but we recommend to set it. MotherDuck will create this database (in this case `dlt_data_3`) for you.

Alternatively you can use the connection string syntax
```toml
[destination]
motherduck.credentials="md:///dlt_data_3?token=<my service token>"
```

**3. Run the pipeline**
```
python3 chess_pipeline.py
```

## Write disposition
All write dispositions are supported

## Data loading
By default **parquet** files and `COPY` command is used to move files to remote duckdb database. All write dispositions are supported.

**INSERT** format is also supported and will execute a large INSERT queries directly into the remote database. This is way slower and may exceed maximum query size - so not advised.

## dbt support
This destination [integrates with dbt](../transformations/dbt/dbt.md) via [dbt-duckdb](https://github.com/jwills/dbt-duckdb) which is a community supported package. `dbt` version >= 1.5 is required (which is current `dlt` default.)

## Syncing of `dlt` state
This destination fully supports [dlt state sync](../../general-usage/state#syncing-state-with-destination)

## Automated tests
Each destination must pass few hundred automatic tests. MotherDuck is passing those tests (except the transactions OFC). However we encountered issues with ATTACH timeouts when connecting which makes running such number of tests unstable. Tests on CI are disabled.

## Troubleshooting / limitations

### I see a lot of errors in the log like DEADLINE_EXCEEDED or Connection timed out
Motherduck is very sensitive to quality of the internet connection and **number of workers used to load data**. Decrease the number of workers and make sure your internet connection really works. We could not find any way to increase those timeouts yet.


### MotherDuck does not support transactions.
Do not use `begin`, `commit` and `rollback` on `dlt` **sql_client** or on duckdb dbapi connection. It has no effect for DML statements (they are autocommit). It is confusing the query engine for DDL (tables not found etc.).
If your connection if of poor quality and you get a time out when executing DML query it may happen that your transaction got executed,


### I see some exception with home_dir missing when opening `md:` connection.
Some internal component (HTTPS) requires **HOME** env variable to be present. Export such variable to the command line. Here is what we do in our tests:
```python
os.environ["HOME"] = "/tmp"
```
before opening connection

### I see some watchdog timeouts.
We also see them.
```
'ATTACH_DATABASE': keepalive watchdog timeout
```
My observation is that if you write a lot of data into the database then close the connection and then open it again to write, there's a chance of such timeout. Possible **WAL** file is being written to the remote duckdb database.

### Invalid Input Error: Initialization function "motherduck_init" from file
Use `duckdb 0.8.1` or above.
