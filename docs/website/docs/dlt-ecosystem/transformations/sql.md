---
title: Access and transform data with SQL
description: Access and transform the data loaded by a dlt pipeline with the dlt SQL client
keywords: [transform, sql, sql client, dml]
---

# The `dlt` SQL client

Most `dlt` destinations use an implementation of the `SqlClientBase` class to connect to the physical destination to which your data is loaded. DDL statements, data insert or update commands, as well as SQL merge and replace queries, are executed via a connection on this client. It also is used for reading data for the [dashboard app](../../hub/ingestion/dashboard.md) and [data access via `dlt` datasets](../../general-usage/dataset-access/dataset.md).

All SQL destinations make use of an SQL client; additionally, the filesystem has a special implementation of the SQL client which you can read about [below](#the-filesystem-sql-client).

:::note
This page contains technical details about the implementation of the SQL client as well as information on how to use low-level APIs. If you simply want to query your data, it's advised to use [`dlt` datasets](../../general-usage/dataset-access/dataset.md) or the [dashboard app](../../hub/ingestion/dashboard.md).
:::

## Executing a query on the SQL client

You can access the SQL client of your destination via the `sql_client` method on your pipeline. The code below shows how to use the SQL client to execute a query.

```py
pipeline = dlt.pipeline(destination="bigquery", dataset_name="crm")
with pipeline.sql_client() as client:
    with client.execute_query(
        "SELECT id, name, email FROM customers WHERE id = %s",
        10
    ) as cursor:
        # get all data from the cursor as a list of tuples
        print(cursor.fetchall())
```

## Retrieving the data in different formats

The cursor returned by `execute_query` has several methods for retrieving the data. The supported formats are Python tuples, Pandas DataFrame, and Arrow table.

The code below shows how to retrieve the data as a Pandas DataFrame and then manipulate it in memory:

```py
pipeline = dlt.pipeline(pipeline_name="my_pipeline", destination="duckdb")
with pipeline.sql_client() as client:
    with client.execute_query(
        'SELECT "reactions__+1", "reactions__-1", reactions__laugh, reactions__hooray, reactions__rocket FROM issues'
    ) as cursor:
        # calling `df` on a cursor, returns the data as a pandas DataFrame
        reactions = cursor.df()
counts = reactions.sum(0).sort_values(0, ascending=False)
```

## Supported methods on the cursor

- `fetchall()`: returns all rows as a list of tuples;
- `fetchone()`: returns a single row as a tuple;
- `fetchmany(size=None)`: returns a number of rows as a list of tuples; if no size is provided, all rows are returned;
- `df(chunk_size=None, **kwargs)`: returns the data as a Pandas DataFrame; if `chunk_size` is provided, the data is retrieved in chunks of the given size;
- `arrow(chunk_size=None, **kwargs)`: returns the data as an Arrow table; if `chunk_size` is provided, the data is retrieved in chunks of the given size;
- `iter_fetch(chunk_size: int)`: iterates over the data in chunks of the given size as lists of tuples;
- `iter_df(chunk_size: int)`: iterates over the data in chunks of the given size as Pandas DataFrames;
- `iter_arrow(chunk_size: int)`: iterates over the data in chunks of the given size as Arrow tables.

:::info
Which retrieval method you should use very much depends on your use case and the destination you are using. Some drivers for our destinations provided by their vendors natively support Arrow or Pandas DataFrames; in these cases, we will use that interface. If they do not, `dlt` will convert lists of tuples into these formats.
:::

## The filesystem SQL client

The filesystem destination implements a special but extremely useful version of the SQL client. While during a normal pipeline run, the filesystem does not make use of an SQL client but rather copies the files resulting from a load into the folder or bucket you have specified, it is possible to query this data using SQL via this client. For this to work, `dlt` uses an in-memory `DuckDB` database instance and makes your filesystem tables available as views on this database. For the most part, you can use the filesystem SQL client just like any other SQL client. `dlt` uses sqlglot to discover which tables you are trying to access and, as mentioned above, `DuckDB` to make them queryable.

The code below shows how to use the filesystem SQL client to query the data:

```py
pipeline = dlt.pipeline(destination="filesystem", dataset_name="my_dataset")
with pipeline.sql_client() as client:
    with client.execute_query("SELECT * FROM my_table") as cursor:
        print(cursor.fetchall())
```

A few things to know or keep in mind when using the filesystem SQL client:

- The SQL database you are actually querying is an in-memory database, so if you do any kind of mutating queries, these will not be persisted to your folder or bucket.
- You must have loaded your data as `JSONL`, `Parquet`, `CSV` files or `delta`/`iceberg` tables for this SQL client to work. For optimal performance, you should use `Parquet` files or open table formats, as `DuckDB` is able to only read the bytes needed to execute your query from a folder or bucket in this case.
- Keep in mind that if you do any filtering, sorting, or full table loading with the SQL client, the in-memory `DuckDB` instance will have to download and query a lot of data from your bucket or folder if you have a large table.
- If you are accessing data on a bucket, `dlt` will temporarily store your credentials in `DuckDB` to let it connect to the bucket.
- Some combinations of buckets and table formats may not be fully supported at this time.
- Multi-schema support (dlt 1.25.0+): When a dataset includes multiple schemas, the filesystem SQL client creates views that span all schemas. If the same table name exists in multiple schemas at different physical locations (e.g. when the layout includes `{schema_name}/`), views are combined. If they share the same location, columns are merged into a single view. This means queries may return rows from multiple schemas — use `pipeline.dataset(schema="name")` to restrict to one schema.

### Control data freshness
`sqlclient` creates views in which the data is immutable (each next query will access the same data). Such "snapshots" are created by:
* globbing the table files once - when view is created
* using the newest iceberg metadata to create view

Updating views may be costly (globbing, re-reading iceberg metadata) so your best option is to create new `sql_client` (or `pipeline.dataset()`) instance
when you need fresh data. Alternatively you can enable autorefresh mode which will re-create view on each query:

```py
from dlt.destination import filesystem

pipeline = dlt.pipeline(destination=filesystem(always_refresh_views=True), dataset_name="my_dataset")
with pipeline.sql_client() as client:
    with client.execute_query("SELECT * FROM my_table") as cursor:
        print(cursor.fetchall())
        # pipeline.run() here and get updated data
        print(cursor.fetchall())
```

Note: `delta` tables are by default on autorefresh which is implemented by delta core and seems to be pretty efficient.

## Transform data with DML statements

A simple alternative to dbt is to query the data using the `dlt` SQL client and then perform the
transformations using SQL statements in Python. The `execute_sql` method allows you to execute any SQL statement,
including statements that change the database schema or data in the tables. In the example below, we
insert a row into the `customers` table. Note that the syntax is the same as for any standard `dbapi`
connection.

:::info
This method will work for all SQL destinations supported by `dlt`, but not for the filesystem destination - its SQL client runs on an in-memory database, so [mutating queries are not persisted](#the-filesystem-sql-client).
:::

Typically you will use this type of transformation if you can create or update tables directly from existing tables
without any need to insert data from your Python environment.

The example below creates a new table `aggregated_sales` that contains the total and average sales for each category and region

```py
pipeline = dlt.pipeline(destination="duckdb", dataset_name="crm")

# NOTE: this is the duckdb sql dialect, other destinations may use different expressions
with pipeline.sql_client() as client:
    client.execute_sql(
        """ CREATE OR REPLACE TABLE aggregated_sales AS
            SELECT
                category,
                region,
                SUM(amount) AS total_sales,
                AVG(amount) AS average_sales
            FROM
                sales
            GROUP BY
                category,
                region;
    """)
```

You can also use the `execute_sql` method to run select queries. The data is returned as a list of rows, with the elements of a row
corresponding to selected columns. A more convenient way to extract data is to use dlt datasets.

```py
try:
    with pipeline.sql_client() as client:
        res = client.execute_sql(
            "SELECT id, name, email FROM customers WHERE id = %s",
            10
        )
        # Prints column values of the first row
        print(res[0])
except Exception:
    ...
```

## Other transforming tools

If you want to transform your data before loading, you can use Python. If you want to transform the
data after loading, you can use SQL or one of the following:

1. [dbt](dbt/dbt.md) (recommended).
2. [Python with DataFrames or Arrow tables](python.md).
