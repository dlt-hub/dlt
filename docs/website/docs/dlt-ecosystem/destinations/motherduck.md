---
title: 🧪 MotherDuck
description: MotherDuck `dlt` destination
keywords: [MotherDuck, duckdb, destination, data warehouse]
---

# MotherDuck

> 🧪 MotherDuck is still invitation only and intensively tested. Please see the limitations / problems at the end.

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

## Data loading
By default **parquet** files and `COPY` command is used to move files to remote duckdb database. All write dispositions are supported.

**INSERT** format is also supported and will execute a large INSERT queries directly into the remote database. This is way slower and may exceed maximum query size - so not advised.

## dbt support
MotherDuck dbt profile got added but until we make `dbt` version 1.5.x working with `dlt`, we can't launch `dbt-duckdb` that supports motherduck.

## Automated tests
Each destination must pass few hundred automatic tests. MotherDuck is passing those tests (except the transactions OFC). However we encountered issues with ATTACH timeouts when connecting which makes running such number of tests unstable. Tests on CI are disabled.

## Troubleshooting / limitations

### MotherDuck does not support transactions.
Do not use `begin`, `commit` and `rollback` on `dlt` **sql_client** or on duckdb dbapi connection. It has no effect for DML statements (they are autocommit). It is confusing the query engine for DDL (tables not found etc.). It simply does not work

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
Use `duckdb 0.8.1`

