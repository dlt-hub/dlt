---
title: The SQL Client
description: Technical details about the destination sql client
keywords: [data, dataset, sql]
---

# The SQL client

:::note
This page contains technical details about the implementation of the SQL client as well as information on how to use low-level APIs. If you simply want to query your data, it's advised to read the pages in this section on accessing data via `dlt` datasets, Streamlit, or Ibis.
:::

Most `dlt` destinations use an implementation of the `SqlClientBase` class to connect to the physical destination to which your data is loaded. DDL statements, data insert or update commands, as well as SQL merge and replace queries, are executed via a connection on this client. It also is used for reading data for the [Streamlit app](./streamlit.md) and [data access via `dlt` datasets](./dataset.md).

All SQL destinations make use of an SQL client; additionally, the filesystem has a special implementation of the SQL client which you can read about [below](#the-filesystem-sql-client).

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
- You must have loaded your data as `JSONL` or `Parquet` files for this SQL client to work. For optimal performance, you should use `Parquet` files, as `DuckDB` is able to only read the bytes needed to execute your query from a folder or bucket in this case.
- Keep in mind that if you do any filtering, sorting, or full table loading with the SQL client, the in-memory `DuckDB` instance will have to download and query a lot of data from your bucket or folder if you have a large table.
- If you are accessing data on a bucket, `dlt` will temporarily store your credentials in `DuckDB` to let it connect to the bucket.
- Some combinations of buckets and table formats may not be fully supported at this time.

