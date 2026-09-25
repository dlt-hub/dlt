---
title: SQL database via SQLAlchemy
description: SQLAlchemy destination
keywords: [sql, sqlalchemy, database, destination]
---
# SQLAlchemy destination

The SQLAlchemy destination allows you to use any database that has an [SQLAlchemy dialect](https://docs.sqlalchemy.org/en/20/dialects/) implemented as a destination.

Currently, MySQL, SQLite, and DuckDB are considered to have full support and are tested as part of the `dlt` CI suite. Other dialects are not tested but should generally work.

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

1. Initialize a project with a pipeline that loads to MS SQL by running:

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

Some dialects need connection URL query parameters on top of the fields above, for example Oracle's `service_name`. Read
[Oracle connection URL query parameters](#oracle-connection-url-query-parameters) to see how to pass them from `secrets.toml`
and from environment variables.

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

### Passing SQLAlchemy Engine Options: `engine_kwargs`

The SQLAlchemy destination accepts an optional `engine_kwargs` parameter, which is forwarded directly to `sqlalchemy.create_engine`.
The equivalent `engine_args` parameter is maintained for backward compatibility, but will be removed in a future release.

Example enabling SQLAlchemy verbose logging:

#### In `.dlt/secrets.toml`

```toml
[destination.sqlalchemy]
credentials = "sqlite:///logger.db"
```

#### In `.dlt/config.toml`

```toml
[destination.sqlalchemy.engine_kwargs]
echo = true
```

Or, directly in code:

```py
import logging
import dlt
from dlt.destinations import sqlalchemy

logging.basicConfig(level=logging.INFO)

dest = sqlalchemy(
    credentials="sqlite:///logger.db",
    engine_kwargs={"echo": True},
)

pipeline = dlt.pipeline(
    pipeline_name='logger',
    destination=dest,
    dataset_name='main'
)

pipeline.run(
    [
        {'id': 1},
        {'id': 2},
        {'id': 3},
    ],
    table_name="logger"
)
```

Here, `engine_kwargs` configures only the engine used by SQLAlchemy as a **destination**. It does not affect resource extraction (use `engine_kwargs` for sql sources, see [here](../verified-sources/sql_database/configuration.md#passing-sqlalchemy-engine-options-engine_kwargs)).

### Session timezone

`sqlalchemy` has no `session_timezone` setting, unlike the other SQL destinations. The way to set one
is dialect-specific, and SQLite has no timezone concept. On MySQL, set the timezone on each new
connection with `connect_args`:

```toml
[destination.sqlalchemy.engine_kwargs.connect_args]
init_command = "SET time_zone = '+00:00'"
```

Named zones such as `Europe/Berlin` work only on a MySQL server with populated timezone tables. A UTC
offset always works.

## Notes on SQLite

### Dataset files

When using an SQLite database file, each dataset is stored in a separate file since SQLite does not support multiple schemas in a single database file.
Under the hood, this uses [`ATTACH DATABASE`](https://www.sqlite.org/lang_attach.html).

The file is stored in the same directory as the main database file (provided by your database URL).

E.g., if your SQLite URL is `sqlite:////home/me/data/chess_data.db` and your `dataset_name` is `games`, the data
is stored in `/home/me/data/chess_data__games.db`

**Note**: If the dataset name is `main`, no additional file is created as this is the default SQLite database.

### In-memory SQLite

In-memory databases require a persistent connection as the database is destroyed when the connection is closed.
Normally, connections are opened and closed for each load job and in other stages during the pipeline run.
To ensure the database persists throughout the pipeline run, you need to pass in an SQLAlchemy `Engine` object instead of credentials.
This engine is not disposed of automatically by `dlt`.

#### Shared-cache URI mode (recommended)

The recommended approach uses SQLite's [shared-cache URI](https://www.sqlite.org/inmemorydb.html) format.
This creates a named in-memory database that can be safely accessed by multiple connections across threads
via `SingletonThreadPool` (one connection per thread):

```py
import dlt
import sqlalchemy as sa

engine = sa.create_engine(
    "sqlite:///file:shared?mode=memory&cache=shared&uri=true",
    connect_args={"check_same_thread": False},
    poolclass=sa.pool.SingletonThreadPool,
)

pipeline = dlt.pipeline(
    "my_pipeline",
    destination=dlt.destinations.sqlalchemy(engine),
    dataset_name="main",
)

pipeline.run([1, 2, 3], table_name="my_table")

with engine.connect() as conn:
    result = conn.execute(sa.text("SELECT * FROM my_table"))
    print(result.fetchall())

engine.dispose()
```

- `mode=memory&cache=shared` creates a named in-memory database shared across all connections in the process.
- `uri=true` is required for `pysqlite` to interpret the database path as a URI.
- `SingletonThreadPool` gives each thread its own connection while all threads see the same data.

#### StaticPool with single worker

Alternatively, you can use `StaticPool` (single shared connection) with `workers=1` to avoid concurrent
access on the same connection:

```py
import dlt
import sqlalchemy as sa

engine = sa.create_engine(
    "sqlite:///:memory:",
    connect_args={"check_same_thread": False},
    poolclass=sa.pool.StaticPool,
)

pipeline = dlt.pipeline(
    "my_pipeline",
    destination=dlt.destinations.sqlalchemy(engine),
    dataset_name="main",
)

pipeline.run([1, 2, 3], table_name="my_table", loader_file_format="typed-jsonl")

with engine.connect() as conn:
    result = conn.execute(sa.text("SELECT * FROM my_table"))
    print(result.fetchall())

engine.dispose()
```

```toml
[load]
workers=1
```

:::caution
With `StaticPool`, all threads share a single underlying database connection.
Using the default parallel loader (`workers > 1`) can cause race conditions where
committed data appears missing. Always set `workers=1` when using `StaticPool`.
:::

### Database locking with `ATTACH DATABASE` on Windows

When `dataset_name` is not `main`, dlt uses SQLite's `ATTACH DATABASE` to store each dataset in a separate file. On Windows, a second `ATTACH` on the same connection can lock indefinitely under concurrent access (e.g. when using the default parallel loading strategy).

To work around this issue, use one of the following approaches:

1. **Set `dataset_name` to `main`** so that no `ATTACH` is needed:

   ```py
   pipeline = dlt.pipeline(
       pipeline_name='my_pipeline',
       destination=dlt.destinations.sqlalchemy(credentials="sqlite:///my_data.db"),
       dataset_name='main'
   )
   ```

2. **Use sequential loading** to avoid concurrent `ATTACH` calls:

   ```toml
   [load]
   workers=1
   ```

## Notes on DuckDB

Install the [duckdb_engine](https://github.com/Mause/duckdb_engine) dialect to use DuckDB with the SQLAlchemy destination:

```sh
pip install duckdb-engine
```

```py
import dlt

pipeline = dlt.pipeline(
    pipeline_name="my_pipeline",
    destination=dlt.destinations.sqlalchemy(credentials="duckdb:///my_data.duckdb"),
    dataset_name="my_dataset",
)
```

Relative database paths are placed in the pipeline's local directory, same as for SQLite and the native DuckDB destination.

**Note**: Prefer the native [DuckDB destination](duckdb.md). Use SQLAlchemy when you need full control over the engine, e.g., to `ATTACH` encrypted database files with connection setup SQL.

### In-memory DuckDB

An in-memory DuckDB database is private to each connection. Pass an `Engine` that shares a single connection and load sequentially:

```py
import dlt
import sqlalchemy as sa

engine = sa.create_engine("duckdb:///:memory:", poolclass=sa.pool.StaticPool)

pipeline = dlt.pipeline(
    pipeline_name="my_pipeline",
    destination=dlt.destinations.sqlalchemy(engine),
    dataset_name="my_dataset",
)
```

```toml
[load]
workers=1
```

### DuckDB dialect limitations

* `duckdb_engine` does not reflect primary key and unique constraints nor indexes: they are created when `create_primary_keys` / `create_unique_indexes` are enabled but cannot be read back with SQLAlchemy tooling.
* JSON columns are reflected as VARCHAR.
* VARCHAR precision is accepted in DDL but not stored by DuckDB.
* Parquet files are loaded with batch INSERT statements: ADBC ingestion is not implemented for this dialect.

## Notes on Oracle

Install the [python-oracledb](https://oracle.github.io/python-oracledb/) driver and use the `oracle+oracledb` dialect in your
connection URL:

```sh
pip install oracledb
```

### Oracle connection URL query parameters

Oracle instances are usually addressed by a service name, and SQLAlchemy takes it as a query parameter of the connection URL,
not as a connection field. In `secrets.toml`, query parameters live in their own table:

```toml
[destination.sqlalchemy.credentials]
drivername = "oracle+oracledb"
username = "loader"
password = "<password>"
host = "orahost"
port = 1521

[destination.sqlalchemy.credentials.query]
service_name = "svc.example.com"
```

There is no environment variable for a single query parameter. `query` is one credentials field holding a dictionary, and the
environment provider reads one variable per field instead of walking into the dictionary. A variable such as
`DESTINATION__SQLALCHEMY__CREDENTIALS__QUERY__SERVICE_NAME` is therefore never read, the credentials resolve with `query` unset,
and `dlt` connects without the service name.

Two forms do work. Pass the whole connection URL in one variable, with the query parameters appended to it:

```sh
export DESTINATION__SQLALCHEMY__CREDENTIALS="oracle+oracledb://loader:<password>@orahost:1521/?service_name=svc.example.com"
```

Or keep the fields separate and pass the entire `query` dictionary as the value of one variable:

```sh
export DESTINATION__SQLALCHEMY__CREDENTIALS__DRIVERNAME="oracle+oracledb"
export DESTINATION__SQLALCHEMY__CREDENTIALS__USERNAME="loader"
export DESTINATION__SQLALCHEMY__CREDENTIALS__PASSWORD="<password>"
export DESTINATION__SQLALCHEMY__CREDENTIALS__HOST="orahost"
export DESTINATION__SQLALCHEMY__CREDENTIALS__PORT="1521"
export DESTINATION__SQLALCHEMY__CREDENTIALS__QUERY='{"service_name": "svc.example.com"}'
```

Both resolve to the same URL, `oracle+oracledb://loader:***@orahost:1521/?service_name=svc.example.com`, and any other parameter
your listener needs (`sid`, `encoding`, `events`) is passed the same way.

:::caution
The `query` variable must hold valid JSON with double quotes. A Python-style value such as `{'service_name': 'svc.example.com'}`
is rejected with `ConfigValueCannotBeCoercedException`. In the single-URL form, URL-encode characters like `@`, `/`, `?` and `#`
in the password.
:::

### Oracle merge and staging datasets

`merge` (both the `delete-insert` and the `scd2` strategy) and `replace` with the `insert-from-staging` or `staging-optimized`
strategy first load data into a [staging dataset](../staging.md#staging-dataset) and then modify the final tables from there. The
staging dataset is a second schema, named `<dataset_name>_staging` by default, and `dlt` creates it and writes to it **over the
same connection as the final dataset**. There is no second credential involved: the SQLAlchemy destination cannot be combined
with a `staging=` destination at all, because that slot is for [file staging](../staging.md#staging-storage) and
`dlt.pipeline(staging=dlt.destinations.sqlalchemy(...))` raises `DestinationNoStagingMode`.

In Oracle, a schema is a user, and a user can create tables only in the schema it owns. Creating the staging tables in any other
schema requires the `CREATE ANY TABLE` system privilege, plus the privileges for the statements `dlt` then runs against those
tables (`INSERT`, `SELECT`, `DELETE`, `DROP`). A user that owns schema `LOADER` cannot create tables in `LOADER_STAGING` without
them, so a merge load into Oracle needs one of the following:

1. **A pre-created staging schema.** `dlt` issues `CREATE SCHEMA <staging_dataset>` only when the schema is missing, and that
   statement does not create a user in Oracle. Ask a DBA to create the schema (user) up front and point `dlt` at it with a fixed
   `staging_dataset_name_layout`. A layout without the `%s` placeholder is used as the full name, so every dataset shares one
   staging schema:

   ```py
   import dlt

   dest_ = dlt.destinations.sqlalchemy(staging_dataset_name_layout="analytics_staging")

   pipeline = dlt.pipeline(
       pipeline_name="oracle_merge",
       destination=dest_,
       dataset_name="analytics",
   )
   ```

   The same setting works in `config.toml`:

   ```toml
   [destination.sqlalchemy]
   staging_dataset_name_layout = "analytics_staging"
   ```

2. **The privileges on that schema.** Pre-creating it removes the schema creation step, not the table creation, so the connecting
   user still needs `CREATE ANY TABLE` and the object privileges above unless it owns the staging schema itself.

3. **No staging dataset at all.** `append`, and `replace` with the `truncate-and-insert` strategy (the default for this
   destination), write straight into the final tables and never touch a second schema. Use them when `CREATE ANY TABLE` is out of
   reach.

:::note
`dlt` normalizes dataset names with the schema's [naming convention](../../general-usage/naming-convention.md), so
`ANALYTICS_STAGING` becomes `analytics_staging`. That is the name you want: SQLAlchemy renders an all-lowercase identifier
unquoted and Oracle folds it to the upper-case schema `ANALYTICS_STAGING`, while a quoted mixed- or upper-case name has to match
the stored name exactly. Set `enable_dataset_name_normalization = false` on the destination if you must keep the case you wrote.
:::

### Oracle limitations

* In Oracle, regular (non-DBA, non-SYS/SYSOPS) users are assigned one schema on user creation, and usually cannot create other schemas. For features requiring staging datasets you should either ensure schema creation rights for the DB user or exactly specify existing schema to be used for staging dataset. See [staging dataset documentation](../staging.md#staging-dataset) for more details

## Notes on other dialects

We tested this destination on **mysql**, **sqlite**, **duckdb**, **oracledb** and **mssql** dialects. Below are a few notes that may help enabling other dialects:

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

#### Quick approach: pass `type_mapper` directly

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

    def _db_type_from_json_type(self, column, table=None):
        return JSONString()

# pass dest_ in `destination` argument to dlt.pipeline
dest_ = dlt.destinations.sqlalchemy(type_mapper=TrinoTypeMapper)
```

The `SqlalchemyTypeMapper` dispatches to per-type visitor methods (`db_type_from_text_type`, `db_type_from_json_type`, `db_type_from_bool_type`, etc.), so you only need to override the type(s) you want to customize. You can also override `to_destination_type()` directly for full control.

Custom type mapper is also useful when you want to limit the length of the string. Below we are adding variant
for `mssql` dialect:

```py
import sqlalchemy as sa
from dlt.common.schema.typing import PreparedTableSchema
from dlt.destinations.impl.sqlalchemy.type_mapper import SqlalchemyTypeMapper

class CustomMssqlTypeMapper(SqlalchemyTypeMapper):
    """This is only an illustration, `sqlalchemy` destination already handles mssql types"""

    def db_type_from_text_type(self, column, table: PreparedTableSchema):
        type_ = super().db_type_from_text_type(column, table)
        length = column.get("precision")
        if length is None:
            return type_.with_variant(sa.UnicodeText(), "mssql")
        else:
            return type_.with_variant(sa.Unicode(length=length), "mssql")
```

:::warning
When extending type mapper for mssql, mysql and trino start with MssqlVariantTypeMapper, MysqlVariantTypeMapper and
TrinoVariantTypeMapper respectively
:::

#### Full approach: register custom dialect capabilities

For a more comprehensive integration, you can register a `DialectCapabilities` class for your database backend. This allows you to customize type mapping, destination capabilities, table structure, and error handling — all in one place. Registered capabilities are automatically applied when the SQLAlchemy destination connects to a matching database.

```py
from typing import Optional, Type

import sqlalchemy as sa
from dlt.common.destination.capabilities import DataTypeMapper, DestinationCapabilitiesContext
from dlt.common.destination.typing import PreparedTableSchema
from dlt.destinations.impl.sqlalchemy.dialect import (
    DialectCapabilities,
    register_dialect_capabilities,
)
from dlt.destinations.impl.sqlalchemy.type_mapper import SqlalchemyTypeMapper


class MyTypeMapper(SqlalchemyTypeMapper):
    """Override only the types you need to customize."""

    def _db_type_from_json_type(self, column, table=None):
        # store JSON as VARCHAR instead of native JSON
        return sa.String(length=4000)


class MyDialectCapabilities(DialectCapabilities):
    def adjust_capabilities(
        self, caps: DestinationCapabilitiesContext, dialect: sa.engine.interfaces.Dialect
    ) -> None:
        caps.max_identifier_length = 128
        caps.max_column_identifier_length = 128
        caps.sqlglot_dialect = "oracle"  # type: ignore[assignment]

    def type_mapper_class(self) -> Type[DataTypeMapper]:
        return MyTypeMapper

    def adapt_table(
        self, table: sa.Table, table_schema: PreparedTableSchema
    ) -> sa.Table:
        # Example: reorder columns so primary key columns come first.
        # Some databases (e.g. StarRocks) require this ordering.
        pk_col_names = [c.name for c in table.primary_key.columns]
        if not pk_col_names:
            return table
        pk_cols = [c for c in table.columns if c.name in pk_col_names]
        other_cols = [c for c in table.columns if c.name not in pk_col_names]
        if [c.name for c in table.columns] == [c.name for c in pk_cols + other_cols]:
            return table  # already in order
        schema = table.schema
        name = table.name
        metadata = table.metadata
        metadata.remove(table)
        return sa.Table(
            name, metadata,
            *[c.copy() for c in pk_cols + other_cols],
            sa.PrimaryKeyConstraint(*pk_col_names),
            schema=schema,
        )

    def is_undefined_relation(self, e: Exception) -> Optional[bool]:
        # return True if the exception means table/schema doesn't exist
        # return False to prevent default pattern matching
        # return None to fall through to built-in patterns
        if "MY_CUSTOM_MISSING_TABLE_CODE" in str(e):
            return True
        return None


# register for your backend name (as shown in the SQLAlchemy connection URL)
register_dialect_capabilities("my_dialect", MyDialectCapabilities)
```

After registration, any pipeline using a `my_dialect://` connection URL will automatically use the custom capabilities. No additional configuration is needed.

The `DialectCapabilities` class supports four extension points:

| Method                  | Description                                                                                                |
| ----------------------- | ---------------------------------------------------------------------------------------------------------- |
| `adjust_capabilities`   | Modify destination capabilities (identifier lengths, timestamp precision, sqlglot dialect, etc.)           |
| `type_mapper_class`     | Return a custom `DataTypeMapper` subclass for the dialect                                                  |
| `adapt_table`           | Modify `sa.Table` objects before they are created or used for loading (e.g. reorder columns for StarRocks) |
| `is_undefined_relation` | Classify exceptions as "table/schema not found" errors for the dialect                                     |

:::tip
Passing `type_mapper=` directly to `dlt.destinations.sqlalchemy()` always takes precedence over the registered dialect capabilities. Use direct passing for one-off overrides and registration for reusable dialect support.
:::

## Write dispositions

The following write dispositions are supported:

- `append`
- `replace` with `truncate-and-insert` and `insert-from-staging` replace strategies. `staging-optimized` falls back to `insert-from-staging`.
- `merge` with `delete-insert` and `scd2` merge strategies.

## Data loading

### Fast loading with parquet

[parquet](../file-formats.md#parquet) file format is supported via [ADBC driver](https://arrow.apache.org/adbc/) for **mysql**.
The driver is provided by [Columnar](https://columnar.tech/). To install it you'll need `dbc` which is a tool to manage ADBC drivers:

```sh
pip install adbc-driver-manager dbc
dbc install mysql
```

with `uv` you can run `dbc` directly:

```sh
uv tool run dbc install mysql
```

You must have the correct driver installed and `loader_file_format` set to `parquet` in order to use ADBC. If driver is not found,
`dlt` will convert parquet into INSERT statements.

We copy parquet files with batches of size of 1 row group. All groups are copied in a single transaction.

:::caution
The ADBC driver is based on go-mysql. We do minimal conversion of connection strings from SQLAlchemy (ssl cert settings for mysql).
:::

#### Why ADBC is not supported for SQLite

ADBC is disabled for SQLite because Python's `sqlite3` module and `adbc_driver_sqlite` bundle different SQLite library versions.
When both libraries operate on the same database file in WAL mode, they have conflicting memory-mapped views of the
WAL index file (`-shm`), causing data corruption. See [TensorBoard issue #1467](https://github.com/tensorflow/tensorboard/issues/1467)
for details on this two-library conflict.

For SQLite, parquet files are loaded using batch INSERT statements instead.

### Loading with SqlAlchemy batch INSERTs

Data is loaded in a dialect-agnostic manner with an `insert` statement generated by SQLAlchemy's core API.
Rows are inserted in batches as long as the underlying database driver supports it. By default, the batch size is 10,000 rows.

## Syncing of `dlt` state

This destination fully supports [dlt state sync](../../general-usage/state#syncing-state-with-destination).

### Data types

All `dlt` data types are supported, but how they are stored in the database depends on the SQLAlchemy dialect.
For example, SQLite does not have `DATETIME` or `TIMESTAMP` types, so `timestamp` columns are stored as `TEXT` in ISO 8601 format.

## Supported file formats

* [typed-jsonl](../file-formats.md#jsonl) is used by default. JSON-encoded data with typing information included.
* [Parquet](../file-formats.md#parquet) is supported.

## Supported column hints

No indexes or constraints are created on the table. You can enable the following via destination configuration

```toml
[destination.sqlalchemy]
create_unique_indexes=true
create_primary_keys=true
```

* `unique` hints are translated to `UNIQUE` constraints via SQLAlchemy.
* `primary_key` hints are translated to `PRIMARY KEY` constraints via SQLAlchemy.
