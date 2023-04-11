---
title: DuckDB
description: DuckDB `dlt` destination
keywords: [duckdb, destination, data warehouse]
---

# DuckDB

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
python3 chess.py
```

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

You can configure a DuckDB destination with [secret / config values](../general-usage/credentials.md) (e.g. using a `secrets.toml` file)
```toml
destination.duckdb.credentials=duckdb:///_storage/test_quack.duckdb
```