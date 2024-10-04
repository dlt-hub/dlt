---
title: Advanced
description: advance configuration and usage of the sql_database source
keywords: [sql connector, sql database pipeline, sql database]
---

import Header from '../_source-info-header.md';

# Advanced usage

<Header/>

## Incremental loading

Efficient data management often requires loading only new or updated data from your SQL databases, rather than reprocessing the entire dataset. This is where incremental loading comes into play.

Incremental loading uses a cursor column (e.g., timestamp or auto-incrementing ID) to load only data newer than a specified initial value, enhancing efficiency by reducing processing time and resource use. Read [here](../../../walkthroughs/sql-incremental-configuration) for more details on incremental loading with `dlt`.

#### How to configure
1. **Choose a cursor column**: Identify a column in your SQL table that can serve as a reliable indicator of new or updated rows. Common choices include timestamp columns or auto-incrementing IDs.
1. **Set an initial value**: Choose a starting value for the cursor to begin loading data. This could be a specific timestamp or ID from which you wish to start loading data.
1. **Deduplication**: When using incremental loading, the system automatically handles the deduplication of rows based on the primary key (if available) or row hash for tables without a primary key.
1. **Set end_value for backfill**: Set `end_value` if you want to backfill data from a certain range.
1. **Order returned rows**: Set `row_order` to `asc` or `desc` to order returned rows.

#### Examples

1. **Incremental loading with the resource `sql_table`**.

  Consider a table "family" with a timestamp column `last_modified` that indicates when a row was last modified. To ensure that only rows modified after midnight (00:00:00) on January 1, 2024, are loaded, you would set the `last_modified` timestamp as the cursor as follows:

  ```py
  import dlt
  from dlt.sources.sql_database import sql_table
  from dlt.common.pendulum import pendulum

  # Example: Incrementally loading a table based on a timestamp column
  table = sql_table(
     table='family',
     incremental=dlt.sources.incremental(
         'last_modified',  # Cursor column name
         initial_value=pendulum.DateTime(2024, 1, 1, 0, 0, 0)  # Initial cursor value
     )
  )

  pipeline = dlt.pipeline(destination="duckdb")
  info = pipeline.extract(table, write_disposition="merge")
  print(info)
  ```

  Behind the scene, the loader generates a SQL query filtering rows with `last_modified` values greater than the incremental value. In the first run, this is the initial value (midnight (00:00:00) January 1, 2024).
  In subsequent runs, it is the latest value of `last_modified` that `dlt` stores in [state](../../../general-usage/state).

2. **Incremental loading with the source `sql_database`**.

  To achieve the same using the `sql_database` source, you would specify your cursor as follows:

  ```py
  import dlt
  from dlt.sources.sql_database import sql_database

  source = sql_database().with_resources("family")
  # Using the "last_modified" field as an incremental field using initial value of midnight January 1, 2024
  source.family.apply_hints(incremental=dlt.sources.incremental("updated", initial_value=pendulum.DateTime(2022, 1, 1, 0, 0, 0)))

  # Running the pipeline
  pipeline = dlt.pipeline(destination="duckdb")
  info = pipeline.run(source, write_disposition="merge")
  print(info)
  ```

  :::info
    * When using "merge" write disposition, the source table needs a primary key, which `dlt` automatically sets up.
    * `apply_hints` is a powerful method that enables schema modifications after resource creation, like adjusting write disposition and primary keys. You can choose from various tables and use `apply_hints` multiple times to create pipelines with merged, appended, or replaced resources.
  :::

## Parallelized extraction

You can extract each table in a separate thread (no multiprocessing at this point). This will decrease loading time if your queries take time to execute or your network latency/speed is low. To enable this, declare your sources/resources as follows:
```py
from dlt.sources.sql_database import sql_database, sql_table

database = sql_database().parallelize()
table = sql_table().parallelize()
```

## Column reflection
Column reflection is the automatic detection and retrieval of column metadata like column names, constraints, data types, etc. Columns and their data types are reflected with SQLAlchemy. The SQL types are then mapped to `dlt` types.
Depending on the selected backend, some of the types might require additional processing.

The `reflection_level` argument controls how much information is reflected:

- `reflection_level = "minimal"`: Only column names and nullability are detected. Data types are inferred from the data.
- `reflection_level = "full"`: Column names, nullability, and data types are detected. For decimal types, we always add precision and scale. **This is the default.**
- `reflection_level = "full_with_precision"`: Column names, nullability, data types, and precision/scale are detected, also for types like text and binary. Integer sizes are set to bigint and to int for all other types.

If the SQL type is unknown or not supported by `dlt`, then, in the pyarrow backend, the column will be skipped, whereas in the other backends the type will be inferred directly from the data irrespective of the `reflection_level` specified. In the latter case, this often means that some types are coerced to strings and `dataclass` based values from sqlalchemy are inferred as `json` (JSON in most destinations).
:::tip
If you use reflection level **full** / **full_with_precision**, you may encounter a situation where the data returned by sqlalchemy or pyarrow backend does not match the reflected data types. The most common symptoms are:
1. The destination complains that it cannot cast one type to another for a certain column. For example, `connector-x` returns TIME in nanoseconds
and BigQuery sees it as bigint and fails to load.
2. You get `SchemaCorruptedException` or another coercion error during the `normalize` step.
In that case, you may try **minimal** reflection level where all data types are inferred from the returned data. From our experience, this prevents
most of the coercion problems.
:::

You can also override the SQL type by passing a `type_adapter_callback` function. This function takes a `SQLAlchemy` data type as input and returns a new type (or `None` to force the column to be inferred from the data) as output.

This is useful, for example, when:
- You're loading a data type that is not supported by the destination (e.g., you need JSON type columns to be coerced to string).
- You're using a sqlalchemy dialect that uses custom types that don't inherit from standard sqlalchemy types.
- For certain types, you prefer `dlt` to infer the data type from the data and you return `None`.

In the following example, when loading timestamps from Snowflake, you ensure that they get translated into standard sqlalchemy `timestamp` columns in the resultant schema:

```py
import dlt
import sqlalchemy as sa
from dlt.sources.sql_database import sql_database, sql_table
from snowflake.sqlalchemy import TIMESTAMP_NTZ

def type_adapter_callback(sql_type):
    if isinstance(sql_type, TIMESTAMP_NTZ):  # Snowflake does not inherit from sa.DateTime
        return sa.DateTime(timezone=True)
    return sql_type  # Use default detection for other types

source = sql_database(
    "snowflake://user:password@account/database?&warehouse=WH_123",
    reflection_level="full",
    type_adapter_callback=type_adapter_callback,
    backend="pyarrow"
)

dlt.pipeline("demo").run(source)
```

## Configuring with TOML or environment variables
You can set most of the arguments of `sql_database()` and `sql_table()` directly in the TOML files or as environment variables. `dlt` automatically injects these values into the pipeline script.

This is particularly useful with `sql_table()` because you can maintain a separate configuration for each table (below we show **secrets.toml** and **config.toml**; you are free to combine them into one):

The examples below show how you can set arguments in any of the TOML files (`secrets.toml` or `config.toml`):
1. Specifying connection string:
    ```toml
    [sources.sql_database]
    credentials="mssql+pyodbc://loader.database.windows.net/dlt_data?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server"
    ```
2. Setting parameters like backend, `chunk_size`, and incremental column for the table `chat_message`:
    ```toml
    [sources.sql_database.chat_message]
    backend="pandas"
    chunk_size=1000

    [sources.sql_database.chat_message.incremental]
    cursor_path="updated_at"
    ```
    This is especially useful with `sql_table()` in a situation where you may want to run this resource for multiple tables. Setting parameters like this would then give you a clean way of maintaining separate configurations for each table.

3. Handling separate configurations for database and individual tables
    When using the `sql_database()` source, you can separately configure the parameters for the database and for the individual tables.
    ```toml
    [sources.sql_database]
    credentials="mssql+pyodbc://loader.database.windows.net/dlt_data?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server"
    schema="data"
    backend="pandas"
    chunk_size=1000

    [sources.sql_database.chat_message.incremental]
    cursor_path="updated_at"
    ```

    The resulting source created below will extract data using the **pandas** backend with **chunk_size** 1000. The table **chat_message** will load data incrementally using the **updated_at** column. All the other tables will not use incremental loading and will instead load the full data.

    ```py
    database = sql_database()
    ```

You'll be able to configure all the arguments this way (except the adapter callback function). [Standard dlt rules apply](../../../general-usage/credentials/setup).

It is also possible to set these arguments as environment variables [using the proper naming convention](../../../general-usage/credentials/setup#naming-convention):
```sh
SOURCES__SQL_DATABASE__CREDENTIALS="mssql+pyodbc://loader.database.windows.net/dlt_data?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server"
SOURCES__SQL_DATABASE__BACKEND=pandas
SOURCES__SQL_DATABASE__CHUNK_SIZE=1000
SOURCES__SQL_DATABASE__CHAT_MESSAGE__INCREMENTAL__CURSOR_PATH=updated_at
```

