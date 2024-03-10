---
title: 🧪 MotherDuck
description: MotherDuck `dlt` destination
keywords: [MotherDuck, duckdb, destination, data warehouse]
---

# MotherDuck
> 🧪 MotherDuck is still invitation-only and is being intensively tested. Please see the limitations/problems at the end.

## Install dlt with MotherDuck
**To install the DLT library with MotherDuck dependencies:**
```
pip install dlt[motherduck]
```

:::tip
If you see a lot of retries in your logs with various timeouts, decrease the number of load workers to 3-5 depending on the quality of your internet connection. Add the following to your `config.toml`:
```toml
[load]
workers=3
```
or export the **LOAD__WORKERS=3** env variable. See more in [performance](../../reference/performance.md)
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

This will install dlt with the **motherduck** extra which contains **duckdb** and **pyarrow** dependencies.

**3. Add your MotherDuck token to `.dlt/secrets.toml`**
```toml
[destination.motherduck.credentials]
database = "dlt_data_3"
password = "<your token here>"
```
Paste your **service token** into the password field. The `database` field is optional, but we recommend setting it. MotherDuck will create this database (in this case `dlt_data_3`) for you.

Alternatively, you can use the connection string syntax.
```toml
[destination]
motherduck.credentials="md:///dlt_data_3?token=<my service token>"
```

**4. Run the pipeline**
```
python3 chess_pipeline.py
```

## Write disposition
All write dispositions are supported.

## Data loading
By default, Parquet files and the `COPY` command are used to move files to the remote duckdb database. All write dispositions are supported.

The **INSERT** format is also supported and will execute large INSERT queries directly into the remote database. This method is significantly slower and may exceed the maximum query size, so it is not advised.

## dbt support
This destination [integrates with dbt](../transformations/dbt/dbt.md) via [dbt-duckdb](https://github.com/jwills/dbt-duckdb), which is a community-supported package. `dbt` version >= 1.5 is required (which is the current `dlt` default.)

## Syncing of `dlt` state
This destination fully supports [dlt state sync](../../general-usage/state#syncing-state-with-destination).

## Automated tests
Each destination must pass a few hundred automatic tests. MotherDuck is passing these tests (except for the transactions, of course). However, we have encountered issues with ATTACH timeouts when connecting, which makes running such a number of tests unstable. Tests on CI are disabled.

## Troubleshooting / limitations

### I see a lot of errors in the log like DEADLINE_EXCEEDED or Connection timed out
MotherDuck is very sensitive to the quality of the internet connection and the **number of workers used to load data**. Decrease the number of workers and ensure your internet connection is stable. We have not found any way to increase these timeouts yet.

### MotherDuck does not support transactions.
Do not use `begin`, `commit`, and `rollback` on `dlt` **sql_client** or on the duckdb dbapi connection. It has no effect on DML statements (they are autocommit). It confuses the query engine for DDL (tables not found, etc.).
If your connection is of poor quality and you get a timeout when executing a DML query, it may happen that your transaction got executed.

### I see some exception with home_dir missing when opening `md:` connection.
Some internal component (HTTPS) requires the **HOME** env variable to be present. Export such a variable to the command line. Here is what we do in our tests:
```python
os.environ["HOME"] = "/tmp"
```
before opening the connection.

### I see some watchdog timeouts.
We also see them.
```
'ATTACH_DATABASE': keepalive watchdog timeout
```
My observation is that if you write a lot of data into the database, then close the connection and then open it again to write, there's a chance of such a timeout. A possible **WAL** file is being written to the remote duckdb database.

### Invalid Input Error: Initialization function "motherduck_init" from file
Use `duckdb 0.8.1` or above.

<!--@@@DLT_SNIPPET_START tuba::motherduck-->
<!--@@@DLT_SNIPPET_END tuba::motherduck-->
<!---
grammarcheck: true
-->
