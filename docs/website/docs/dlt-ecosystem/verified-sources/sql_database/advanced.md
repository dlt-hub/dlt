---
title: Advanced usage
description: advance configuration and usage of the sql_database source
keywords: [sql connector, sql database pipeline, sql database]
---

import Header from '../_source-info-header.md';

# Advanced usage

<Header/>

### Split or partition long incremental loads
If you have a large table with incremental loading set up, you can partition your initial load or split it in a loop. There are two methods to do that:
* **Partitioning** where you split source data in several ranges, load them (possibly in parallel) and then continue to load data incrementally.
* **Split** where you load data sequentially in small chunks

**Partitioning works as follows:**

1. Get the minimum and maximum cursor column value for a table.
2. Split it into N ranges. Here we assume that ranges are evenly populated. Obviously you can write more clever query that will give you partitions with more
or less similar sizes.
3. For each range find min and max cursor value and
use [incremental with `end_value`](../../../general-usage/incremental/cursor.md#using-end_value-for-backfill) for backfill. 
4. You can load each partition in a loop or in parallel (ie. in separate process). Incremental resources with `end_value` set
do not use the state.
5. Continue regular incremental loading with `initial_value` set to the value at the end of the range
and make the start range open to avoid duplicates.

In example below we partition a table with chat messages by day using `updated_at` timestamp column as cursor.
```py
import dlt
from dlt.sources.sql_database import sql_table

# start today
today = pendulum.now().start_of("day")
# load today and past 3 days
num_days = 4

# create date ranges in a loop
days = [today.subtract(days=i) for i in range(num_days - 1, -1, -1)]
date_ranges = [
    {"start": days[i], "end": days[i + 1] if i < num_days - 1 else days[i].add(days=1)}
    for i in range(num_days)
]

# run dlt pipeline 4 times with incremental set to particular days (via initial_value and end_value)
pipeline = dlt.pipeline("test_load_sql_table_partitioned", destination="duckdb")

# load each date range separately, you could also execute each load in a separate process
for _, date_range in enumerate(date_ranges):
    table_partition = sql_table(
        table="chat_message",
        incremental=dlt.sources.incremental(
            "updated_at",
            initial_value=date_range["start"],
            end_value=date_range["end"],
            range_start="open",
            range_end="closed",
        ),
    )
    print(pipeline.run(table_partition))

# run pipeline incrementally with the max day as initial_value and open range
incremental_table = sql_table(
    table="chat_message",
    incremental=dlt.sources.incremental(
        "updated_at",
        initial_value=date_ranges[-1]["end"],  # use the last day as the starting point
        range_start="open",  # skip the last partition’s end value by using an open start boundary as it was loaded by the last partition

    ),
)
pipeline.run(incremental_table)
```
Please read [notes on parallelism](../../../general-usage/incremental/cursor.md#partition-large-backfills)

**Split loading works as follows:**

1. Use `incremental` property with **row_order** set. 
2. Limit the resource by number of pages or time
4. Run pipeline in a loop as long as it is not empty

```py
import dlt
from dlt.sources.sql_database import sql_table

pipeline = dlt.pipeline("test_load_sql_table_split_loading", destination="duckdb")

# create the incremental table resource with row_order to ensure we don't miss rows
incremental_table = sql_table(
    table="chat_message",
    chunk_size=1000,  # in production use large chunk size
    incremental=dlt.sources.incremental(
        "id",  # in most cases you'll use cursor column of timestamp type
        row_order="asc",  # critical to set row_order when doing split loading
        range_start="open",  # use open range to disable deduplication
    ),
)

# we'll load 2 pages, 1000 rows max on each run
while not pipeline.run(incremental_table.add_limit(2)).is_empty:
    pass
```
Note: if you have a table that receives data all the time, `is_empty` may never be false (there's always new data).
Use `pipeline.last_trace.last_normalize_info.row_counts` for more granular exit conditions

### Inclusive and exclusive filtering

By default the incremental filtering is inclusive on the start value side so that
rows with cursor equal to the last run's cursor are fetched again from the database.

The SQL query generated looks something like this (assuming `last_value_func` is `max` and `row_order` is `asc`):

```sql
SELECT * FROM family
WHERE last_modified >= :start_value
ORDER BY last_modified ASC
```

That means some rows overlapping with the previous load are fetched from the database.
Duplicates are then filtered out by dlt using either the primary key or a hash of the row's contents.

This ensures there are no gaps in the extracted sequence. But it does come with some performance overhead,
both due to the deduplication processing and the cost of fetching redundant records from the database.

This is not always needed. If you know that your data does not contain overlapping cursor values then you
can optimize extraction by passing `range_start="open"` to incremental.

This both **disables the deduplication process** and changes the operator used in the SQL `WHERE` clause from `>=` (greater-or-equal) to `>` (greater than), so that no overlapping rows are fetched.

E.g.

```py
table = sql_table(
    table='family',
    incremental=dlt.sources.incremental(
        'last_modified',  # Cursor column name
        initial_value=pendulum.DateTime(2024, 1, 1, 0, 0, 0),  # Initial cursor value
        range_start="open",  # exclude the start value
    )
)
```

It's a good option if:

* The cursor is an auto incrementing ID
* The cursor is a high precision timestamp and two records are never created at exactly the same time
* Your pipeline runs are timed in such a way that new data is not generated during the load

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

If the SQL type is unknown or not supported by `dlt`, then we'll try to infer it from the data.
* `sqlalchemy` follows standard `dlt` inference rules from Python objects. This often means that some types are coerced to strings and `dataclass` based values from sqlalchemy are inferred as `json` (JSON in most destinations).
* `pyarrow` backend will try to infer types from the data using rules built-in in arrow (we just past an array of Python objects and ask for a type). Variant columns are not created by this backend so columns with inconsistent types cannot be loaded by this backend.


:::tip
If you use reflection level **full** / **full_with_precision**, you may encounter a situation where the data returned by sqlalchemy or pyarrow backend does not match the reflected data types. The most common symptoms are:
1. The destination complains that it cannot cast one type to another for a certain column. For example, `connector-x` returns TIME in nanoseconds
and BigQuery sees it as bigint and fails to load.
2. You get `SchemaCorruptedException` or another coercion error during the `normalize` step.
In that case, you may try **minimal** reflection level where all data types are inferred from the returned data. From our experience, this prevents
most of the coercion problems.
:::

### Adapt reflected types to your needs

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

### Remove nullability information
`dlt` adds `NULL`/`NOT NULL` information to reflected schemas in **all reflection levels**. There are cases where you do not want this information to be present
ie.
* if you plan to use replication source that will (soft) delete rows.
* if you expect that columns will be dropped from the source table.

In such cases you can use a table adapter that removes nullability (`dlt` will create nullable tables as a default):

```py
from dlt.sources.sql_database import sql_table, remove_nullability_adapter

read_table = sql_table(
    table="chat_message",
    reflection_level="full_with_precision",
    table_adapter_callback=remove_nullability_adapter,
)
print(read_table.compute_table_schema())
```

You can call `remove_nullability_adapter` from your custom table adapter if you need to combine both.

### Selecting a subset of columns

You can use `table_adapter_callback` to select only specific columns from a table by removing unwanted columns from the table definition.

```py
from dlt.sources.sql_database import sql_database

def table_adapter_callback(table):
    if table.name == 'my_table':
        columns_to_keep = ['id', 'name', 'email']
        for col in list(table._columns):
            if col.name not in columns_to_keep:
                table._columns.remove(col)
    return table

source = sql_database(
    table_names=["my_table"],
    table_adapter_callback=table_adapter_callback
)
```

### Filtering rows

You can use `query_adapter_callback` to filter rows using SQL conditions.
This allows you to include only the data that matches specific criteria.
```py
from dlt.sources.sql_database import sql_database

def query_adapter_callback(query, table):
    if table.name == "family":
        return query.where(table.c.rfam_id.ilike("%bacteria%"))
    return query

source = sql_database(
    table_names=["family"],
    query_adapter_callback=query_adapter_callback,
)
```
:::note
You can combine both `query_adapter_callback` and `table_adapter_callback`  
to filter rows and select specific columns within the same source.  
This works for one or more tables.

```py
source = sql_database(
    table_names=["family"],
    query_adapter_callback=query_adapter_callback,
    table_adapter_callback=table_adapter_callback,
)
```
:::
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

It is also possible to set these arguments as environment variables [using configuration sections](../../../general-usage/credentials/setup#recommended-section-layout):
```sh
SOURCES__SQL_DATABASE__CREDENTIALS="mssql+pyodbc://loader.database.windows.net/dlt_data?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server"
SOURCES__SQL_DATABASE__BACKEND=pandas
SOURCES__SQL_DATABASE__CHUNK_SIZE=1000
SOURCES__SQL_DATABASE__CHAT_MESSAGE__INCREMENTAL__CURSOR_PATH=updated_at
```

### Configure many sources side by side with custom sections
`dlt` allows you to rename any source to place the source configuration into custom section or to have many instances
of the source created side by side. For example:
```py
from dlt.sources.sql_database import sql_database

my_db = sql_database.clone(name="my_db", section="my_db")(table_names=["chat_message"])
print(my_db.name)
```
Here we create a renamed version of the `sql_database` and then instantiate it. Such source will read
credentials from:
```toml
[sources.my_db]
credentials="mssql+pyodbc://loader.database.windows.net/dlt_data?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server"
schema="data"
backend="pandas"
chunk_size=1000

[sources.my_db.chat_message.incremental]
cursor_path="updated_at"
```
