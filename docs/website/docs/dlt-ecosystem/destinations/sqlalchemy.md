---
title: SQL databases (powered by SQLAlchemy)
description: SQLAlchemy destination
keywords: [sql, sqlalchemy, database, destination]
---

# SQLAlchemy destination

The SQLAlchemy destination allows you to use any database which has an [SQLAlchemy dialect](https://docs.sqlalchemy.org/en/20/dialects/) implemented as a destination.

Currently mysql and SQLite are considered to have full support and are tested as part of the `dlt` CI suite. Other dialects are not tested but should generally work.

## Install dlt with SQLAlchemy

Install dlt with the `sqlalchemy` extra dependency:

```sh
pip install "dlt[sqlalchemy]"
```

Note that database drivers are not included and need to be installed separately for
the database you plan on using. For example for MySQL:

```sh
pip install mysqlclient
```

Refer to the [SQLAlchemy documentation on dialects](https://docs.sqlalchemy.org/en/20/dialects/) for info about client libraries required for supported databases.

### Create a pipeline

**1. Initialize a project with a pipeline that loads to MS SQL by running:**
```sh
dlt init chess sqlalchemy
```

**2. Install the necessary dependencies for SQLAlchemy by running:**
```sh
pip install -r requirements.txt
```
or run:
```sh
pip install "dlt[sqlalchemy]"
```

**3. Install your database client library.**

E.g. for MySQL:
```sh
pip install mysqlclient
```

**4. Enter your credentials into `.dlt/secrets.toml`.**

For example, replace with your database connection info:
```toml
[destination.sqlalchemy.credentials]
database = "dlt_data"
username = "loader"
password = "<password>"
host = "localhost"
port = 3306
driver_name = "mysql"
```

Alternatively a valid SQLAlchemy database URL can be used, either in `secrets.toml` or as an environment variable.
E.g.

```toml
[destination.sqlalchemy]
credentials = "mysql://loader:<password>@localhost:3306/dlt_data"
```

or

```sh
export DESTINATION__SQLALCHEMY__CREDENTIALS="mysql://loader:<password>@localhost:3306/dlt_data"
```

An SQLAlchemy `Engine` can also be passed directly by creating an instance of the destination:

```py
import sqlalchemy as sa
import dlt

engine = sa.create_engine('sqlite:///chess_data.db')

pipeline = dlt.pipeline(
    pipeline_name='chess',
    destination=dlt.destinations.sqlalchemy(engine),
    dataset_name='main'
)
```

## Notes on SQLite

### Dataset files
When using an SQLite database file each dataset is stored in a separate file since SQLite does not support multiple schemas in a single database file.
Under the hood this uses [`ATTACH DATABASE`](https://www.sqlite.org/lang_attach.html).

The file is stored in the same directory as the main database file (provided by your database URL)

E.g. if your SQLite URL is `sqlite:////home/me/data/chess_data.db` and you `dataset_name` is `games`, the data
is stored in `/home/me/data/chess_data__games.db`

**Note**: if dataset name is `main` no additional file is created as this is the default SQLite database.

### In-memory databases
In-memory databases require a persistent connection as the database is destroyed when the connection is closed.
Normally connections are opened and closed for each load job and in other stages during the pipeline run.
To make sure the database persists throughout the pipeline run you need to pass in an SQLAlchemy `Engine` object instead of credentials.
This engine is not disposed of automatically by `dlt`. Example:

```py
import dlt
import sqlalchemy as sa

# Create the sqlite engine
engine = sa.create_engine('sqlite:///:memory:')

# Configure the destination instance and create pipeline
pipeline = dlt.pipeline('my_pipeline', destination=dlt.destinations.sqlalchemy(engine), dataset_name='main')

# Run the pipeline with some data
pipeline.run([1,2,3], table_name='my_table')

# engine is still open and you can query the database
with engine.connect() as conn:
    result = conn.execute(sa.text('SELECT * FROM my_table'))
    print(result.fetchall())
```

## Write dispositions

The following write dispositions are supported:

- `append`
- `replace` with `truncate-and-insert` and `insert-from-staging` replace strategies. `staging-optimized` falls back to `insert-from-staging`.

`merge` disposition is not supported and falls back to `append`.

## Data loading
Data is loaded in a dialect agnostic with an `insert` statement generated with SQLAlchemy's core API.
Rows are inserted in batches as long as the underlying database driver supports it. By default the batch size 10,000 rows.

## Syncing of `dlt` state
This destination fully supports [dlt state sync](../../general-usage/state#syncing-state-with-destination).

### Data types
All `dlt` data types are supported, but how they are stored in the database depends on the SQLAlchemy dialect.
For example SQLite does not have a `DATETIME`/`TIMESTAMP` type so `timestamp` columns are stored as `TEXT` in ISO 8601 format.

## Supported file formats
* [typed-jsonl](../file-formats/jsonl.md) is used by default. JSON encoded data with typing information included.
* [parquet](../file-formats/parquet.md) is supported

## Supported column hints
* `unique` hints are translated to `UNIQUE` constraints via SQLAlchemy (granted the database supports it)