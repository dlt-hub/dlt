---
title: Access datasets in Python
description: Conveniently access the data loaded to any destination in Python
keywords: [destination, schema, data, access, retrieval]
---

# Access loaded data in Python

This guide explains how to access and manipulate data that has been loaded into your destination using the `dlt` Python library. After running your pipelines and loading data, you can use the `pipeline.dataset()` and data frame expressions, Ibis or SQL to query the data and read it as records, Pandas frames or Arrow tables.

## Quick start example

Here's a full example of how to retrieve data from a pipeline and load it into a Pandas DataFrame or a PyArrow Table.

<!--@@@DLT_SNIPPET ./dataset_snippets/dataset_snippets.py::quick_start_example-->

## Getting started

Assuming you have a `Pipeline` object (let's call it `pipeline`), you can obtain a `ReadableDataset` and access your tables as `ReadableRelation` objects.

**Note:** The `ReadableDataset` and `ReadableRelation` objects are **lazy-loading**. They will only query and retrieve data when you perform an action that requires it, such as fetching data into a DataFrame or iterating over the data. This means that simply creating these objects does not load data into memory, making your code more efficient.


### Access the dataset

<!--@@@DLT_SNIPPET ./dataset_snippets/dataset_snippets.py::getting_started-->

### Access tables as dataset

You can access tables in your dataset using either attribute access or item access.

<!--@@@DLT_SNIPPET ./dataset_snippets/dataset_snippets.py::accessing_tables-->

## Reading data

Once you have a `ReadableRelation`, you can read data in various formats and sizes.

### Fetch the entire table

:::caution
Loading full tables into memory without limiting or iterating over them can consume a large amount of memory and may cause your program to crash if the table is too large. It's recommended to use chunked iteration or apply limits when dealing with large datasets. 
:::

#### As a Pandas DataFrame

<!--@@@DLT_SNIPPET ./dataset_snippets/dataset_snippets.py::fetch_entire_table_df-->

#### As a PyArrow Table

<!--@@@DLT_SNIPPET ./dataset_snippets/dataset_snippets.py::fetch_entire_table_arrow-->

#### As a list of Python tuples

<!--@@@DLT_SNIPPET ./dataset_snippets/dataset_snippets.py::fetch_entire_table_fetchall-->

## Lazy loading behavior

The `ReadableDataset` and `ReadableRelation` objects are **lazy-loading**. This means that they do not immediately fetch data when you create them. Data is only retrieved when you perform an action that requires it, such as calling `.df()`, `.arrow()`, or iterating over the data. This approach optimizes performance and reduces unnecessary data loading.

## Iterating over data in chunks

To handle large datasets efficiently, you can process data in smaller chunks.

### Iterate as Pandas DataFrames

<!--@@@DLT_SNIPPET ./dataset_snippets/dataset_snippets.py::iterating_df_chunks-->

### Iterate as PyArrow Tables

<!--@@@DLT_SNIPPET ./dataset_snippets/dataset_snippets.py::iterating_arrow_chunks-->

### Iterate as lists of tuples

<!--@@@DLT_SNIPPET ./dataset_snippets/dataset_snippets.py::iterating_fetch_chunks-->

The methods available on the ReadableRelation correspond to the methods available on the cursor returned by the SQL client. Please refer to the [SQL client](./sql-client.md#supported-methods-on-the-cursor) guide for more information.

## Connection Handling

For every call that actually fetches data from the destination, such as `df()`, `arrow()`, `fetchall()` etc., the dataset will open a connection and close it after it has been retrieved or the iterator is completed. You can keep the connection open for multiple requests with the dataset context manager:

<!--@@@DLT_SNIPPET ./dataset_snippets/dataset_snippets.py::context_manager-->

## Special queries

You can use the `row_counts` method to get the row counts of all tables in the destination as a DataFrame.

<!--@@@DLT_SNIPPET ./dataset_snippets/dataset_snippets.py::row_counts-->

## Modifying queries

You can refine your data retrieval by limiting the number of records, selecting specific columns, sorting the results, filtering rows, or chaining these operations.

### Limit the number of records

<!--@@@DLT_SNIPPET ./dataset_snippets/dataset_snippets.py::limiting_records-->

#### Using `head()` to get the first 5 records

<!--@@@DLT_SNIPPET ./dataset_snippets/dataset_snippets.py::head_records-->

### Select specific columns

<!--@@@DLT_SNIPPET ./dataset_snippets/dataset_snippets.py::select_columns-->

### Sort results

<!--@@@DLT_SNIPPET ./dataset_snippets/dataset_snippets.py::order_by-->

### Filter rows

<!--@@@DLT_SNIPPET ./dataset_snippets/dataset_snippets.py::filter-->

### Chain operations

You can combine `select`, `limit`, and other methods.

<!--@@@DLT_SNIPPET ./dataset_snippets/dataset_snippets.py::chain_operations-->

## Modifying queries with ibis expressions

If you install the amazing [ibis](https://ibis-project.org/) library, you can use ibis expressions to modify your queries.

```sh
pip install ibis-framework
```

dlt will then allow you to get an `ibis.UnboundTable` for each table which you can use to build a query with ibis expressions, which you can then execute on your dataset.

<!--@@@DLT_SNIPPET ./dataset_snippets/dataset_snippets.py::ibis_expressions-->

:::warning
A previous version of dlt allowed to use ibis expressions in a slightly different way, allowing users to directly execute and retrieve data on ibis Unbound tables. While this still works, this method should be considered depcrecated and you should udpate your code to the new version suggested above. The old method will be removed in an upcoming release.
:::

You can learn more about the available expressions on the [ibis for sql users](https://ibis-project.org/tutorials/ibis-for-sql-users) page. 

## Supported destinations

All SQL and filesystem destinations supported by `dlt` can utilize this data access interface.

### Reading data from filesystem
For filesystem destinations, `dlt` [uses **DuckDB** under the hood](./sql-client.md#the-filesystem-sql-client) to create views on iceberg and delta tables or from Parquet, JSONL and csv files. This allows you to query data stored in files using the same interface as you would with SQL databases. If you plan on accessing data in buckets or the filesystem a lot this way, it is advised to load data into delta or iceberg tables, as **DuckDB** is able to only load the parts of the data actually needed for the query to work.

:::tip
By default `dlt` will not autorefresh views created on iceberg tables and files when new data is loaded. This prevents wasting resources on
file globbing and reloading iceberg metadata for every query. You can [change this behavior](sql-client.md#control-data-freshness) with `always_refresh_views` flag.

Note: `delta` tables are by default on autorefresh which is implemented by delta core and seems to be pretty efficient.
:::

## Examples

### Fetch one record as a tuple

<!--@@@DLT_SNIPPET ./dataset_snippets/dataset_snippets.py::fetch_one-->

### Fetch many records as tuples

<!--@@@DLT_SNIPPET ./dataset_snippets/dataset_snippets.py::fetch_many-->

### Iterate over data with limit and column selection

**Note:** When iterating over filesystem tables, the underlying DuckDB may give you a different chunk size depending on the size of the parquet files the table is based on.

<!--@@@DLT_SNIPPET ./dataset_snippets/dataset_snippets.py::iterating_with_limit_and_select-->

## Advanced usage

### Using custom SQL queries to create `ReadableRelations`

You can use custom SQL queries directly on the dataset to create a `ReadableRelation`:

<!--@@@DLT_SNIPPET ./dataset_snippets/dataset_snippets.py::custom_sql-->

:::note
When using custom SQL queries with `dataset()`, methods like `limit` and `select` won't work. Include any filtering or column selection directly in your SQL query.
:::


### Loading a `ReadableRelation` into a pipeline table

Since the `iter_arrow` and `iter_df` methods are generators that iterate over the full `ReadableRelation` in chunks, you can use them as a resource for another (or even the same) `dlt` pipeline:

<!--@@@DLT_SNIPPET ./dataset_snippets/dataset_snippets.py::loading_to_pipeline-->

Learn more about [transforming data in Python with Arrow tables or DataFrames](../../dlt-ecosystem/transformations/python).

### Using `ibis` to query data

Visit the [Native Ibis integration](./ibis-backend.md) guide to learn more.

## Important considerations

- **Memory usage:** Loading full tables into memory without iterating or limiting can consume significant memory, potentially leading to crashes if the dataset is large. Always consider using limits or chunked iteration.

- **Lazy evaluation:** `ReadableDataset` and `ReadableRelation` objects delay data retrieval until necessary. This design improves performance and resource utilization.

- **Custom SQL queries:** When executing custom SQL queries, remember that additional methods like `limit()` or `select()` won't modify the query. Include all necessary clauses directly in your SQL statement.

