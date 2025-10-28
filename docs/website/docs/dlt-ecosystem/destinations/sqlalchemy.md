---
title: 30+ SQL databases (powered by SQLAlchemy)
description: SQLAlchemy destination
keywords: [sql, sqlalchemy, database, destination]
---

# SQLAlchemy destination

The SQLAlchemy destination allows you to use any database that has an [SQLAlchemy dialect](https://docs.sqlalchemy.org/en/20/dialects/) implemented as a destination.

Currently, MySQL and SQLite are considered to have full support and are tested as part of the `dlt` CI suite. Other dialects are not tested but should generally work.

## Install dlt with SQLAlchemy

Install dlt with the `sqlalchemy` extra dependency:

```sh
pip install "dlt[sqlalchemy]"
```

Note that database drivers are not included and need to be installed separately for the database you plan on using. For example, for MySQL:

```sh
pip install mysqlclient
```

Refer to the [SQLAlchemy documentation on dialects](https://docs.sqlalchemy.org/en/20/dialects/) for information about client libraries required for supported databases.

<!--@@@DLT_DESTINATION_CAPABILITIES sqlalchemy-->

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

E.g., for MySQL:
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

Alternatively, a valid SQLAlchemy database URL can be used, either in `secrets.toml` or as an environment variable.
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
When using an SQLite database file, each dataset is stored in a separate file since SQLite does not support multiple schemas in a single database file.
Under the hood, this uses [`ATTACH DATABASE`](https://www.sqlite.org/lang_attach.html).

The file is stored in the same directory as the main database file (provided by your database URL).

E.g., if your SQLite URL is `sqlite:////home/me/data/chess_data.db` and your `dataset_name` is `games`, the data
is stored in `/home/me/data/chess_data__games.db`

**Note**: If the dataset name is `main`, no additional file is created as this is the default SQLite database.

### In-memory databases
In-memory databases require a persistent connection as the database is destroyed when the connection is closed.
Normally, connections are opened and closed for each load job and in other stages during the pipeline run.
To ensure the database persists throughout the pipeline run, you need to pass in an SQLAlchemy `Engine` object instead of credentials.
This engine is not disposed of automatically by `dlt`. Example:

```py
import dlt
import sqlalchemy as sa

# Create the SQLite engine
engine = sa.create_engine('sqlite:///:memory:')

# Configure the destination instance and create pipeline
pipeline = dlt.pipeline('my_pipeline', destination=dlt.destinations.sqlalchemy(engine), dataset_name='main')

# Run the pipeline with some data
pipeline.run([1,2,3], table_name='my_table')

# The engine is still open and you can query the database
with engine.connect() as conn:
    result = conn.execute(sa.text('SELECT * FROM my_table'))
    print(result.fetchall())
```

## Notes on other dialects
We tested this destination on **mysql**, **sqlite** and **mssql** dialects. Below are a few notes that may help enabling other dialects:
1. `dlt` must be able to recognize if a database exception relates to non existing entity (like table or schema). We put
some work to recognize those for most of the popular dialects (look for `db_api_client.py`)
2. Primary keys and unique constraints are not created by default to avoid problems with particular dialects.
3. `merge` write disposition uses only `DELETE` and `INSERT` operations to enable as many dialects as possible.

Please report issues with particular dialects. We'll try to make them work.

### Trino limitations
* Trino dialect does not case fold identifiers. Use `snake_case` naming convention only.
* Trino does not support merge/scd2 write disposition (or you somehow create PRIMARY KEYs on engine tables)
* We convert JSON and BINARY types are cast to STRING (dialect seems to have a conversion bug)
* Trino does not support PRIMARY/UNIQUE constraints


### Adapting destination for a dialect
You can adapt destination capabilities for a particular dialect [by passing your custom settings](../../general-usage/destination.md#pass-additional-parameters-and-change-destination-capabilities). In the example below we pass custom `TypeMapper` that
converts `json` data into `text` on the fly.
```py
from dlt.common import json

import dlt
import sqlalchemy as sa
from dlt.destinations.impl.sqlalchemy.type_mapper import SqlalchemyTypeMapper

class JSONString(sa.TypeDecorator):
    """
    A custom SQLAlchemy type that stores JSON data as a string in the database.
    Automatically serializes Python objects to JSON strings on write and
    deserializes JSON strings back to Python objects on read.
    """

    impl = sa.String
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None

        return json.dumps(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return None

        return json.loads(value)

class TrinoTypeMapper(SqlalchemyTypeMapper):
    """Example mapper that plugs custom string type that serialized to from/json

    Note that instance of TypeMapper contains dialect and destination capabilities instance
    for a deeper integration
    """

    def to_destination_type(self, column, table=None):
        if column["data_type"] == "json":
            return JSONString()
        return super().to_destination_type(column, table)

# pass dest_ in `destination` argument to dlt.pipeline
dest_ = dlt.destinations.sqlalchemy(type_mapper=TrinoTypeMapper)
```

Custom type mapper is also useful when ie. you want to limit the length of the string. Below we are adding variant
for `mssql` dialect:
```py
import sqlalchemy as sa
from dlt.destinations.impl.sqlalchemy.type_mapper import SqlalchemyTypeMapper

class CustomMssqlTypeMapper(SqlalchemyTypeMapper):
    """This is only an illustration, `sqlalchemy` destination already handles mssql types"""

    def to_destination_type(self, column, table=None):
        type_ = super().to_destination_type(column, table)
        if column["data_type"] == "text":
            length = precision = column.get("precision")
            if length is None:
                return type_.with_variant(sa.UnicodeText(), "mssql")  # type: ignore[no-any-return]
            else:
                return type_.with_variant(sa.Unicode(length=length), "mssql")  # type: ignore[no-any-return]
        return type_
```

:::warning
When extending type mapper for mssql, mysql and trino start with MssqlVariantTypeMapper, MysqlVariantTypeMapper and
TrinoVariantTypeMapper respectively
:::


## Write dispositions

The following write dispositions are supported:

- `append`
- `replace` with `truncate-and-insert` and `insert-from-staging` replace strategies. `staging-optimized` falls back to `insert-from-staging`.
- `merge` with `delete-insert` and `scd2` merge strategies.

## Data loading

Data is loaded in a dialect-agnostic manner with an `insert` statement generated by SQLAlchemy's core API.
Rows are inserted in batches as long as the underlying database driver supports it. By default, the batch size is 10,000 rows.

## Syncing of `dlt` state

This destination fully supports [dlt state sync](../../general-usage/state#syncing-state-with-destination).

### Data types

All `dlt` data types are supported, but how they are stored in the database depends on the SQLAlchemy dialect.
For example, SQLite does not have `DATETIME` or `TIMESTAMP` types, so `timestamp` columns are stored as `TEXT` in ISO 8601 format.

## Supported file formats

* [typed-jsonl](../file-formats/jsonl.md) is used by default. JSON-encoded data with typing information included.
* [Parquet](../file-formats/parquet.md) is supported.

## Supported column hints
No indexes or constraints are created on the table. You can enable the following via destination configuration
```toml
[destination.sqlalchemy]
create_unique_indexes=true
create_primary_keys=true
```
* `unique` hints are translated to `UNIQUE` constraints via SQLAlchemy.
* `primary_key` hints are translated to `PRIMARY KEY` constraints via SQLAlchemy.
