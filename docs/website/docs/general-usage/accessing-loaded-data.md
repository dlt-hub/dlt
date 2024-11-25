---
title: Accessing loaded data
description: Conveniently accessing the data loaded to any destination
keywords: [destination, schema, data, access, retrieval]
---


# Accessing Loaded Data with `dlt` in Python

:::caution
All interfaces mentioned on this page are considered experimental and are very likely to change in the next while. This message will be removed as soon as we consider the dataset accessing to be stable :)
:::

This guide explains how to access and manipulate data that has been loaded into your destination using the `dlt` Python library. After running your pipelines and loading data, you can use the `ReadableDataset` and `ReadableRelation` classes to interact with your data programmatically.

**Note:** The `ReadableDataset` and `ReadableRelation` objects are **lazy-loading**. They will only query and retrieve data when you perform an action that requires it, such as fetching data into a DataFrame or iterating over the data. This means that simply creating these objects does not load data into memory, making your code more efficient.

## Quick Start Example

Here's a full example of how to retrieve data from a pipeline and load it into a Pandas DataFrame or a PyArrow Table.

```python
# Assuming you have a Pipeline object named 'pipeline'
# and you have loaded data to a table named 'items' in the destination

# Step 1: Get the readable dataset from the pipeline
dataset = pipeline._dataset()

# Step 2: Access a table as a ReadableRelation
items_relation = dataset.items  # Or dataset["items"]

# Step 3: Fetch the entire table as a Pandas DataFrame
df = items_relation.df()

# Alternatively, fetch as a PyArrow Table
arrow_table = items_relation.arrow()
```

**Caution:** Loading full tables into memory without limiting or iterating over them can consume a large amount of memory and may cause your program to crash if the table is too large. It's recommended to use chunked iteration or apply limits when dealing with large datasets.

## Getting Started

Assuming you have a `Pipeline` object (let's call it `pipeline`), you can obtain a `ReadableDataset` and access your tables as `ReadableRelation` objects.

### Access the `ReadableDataset`

```python
# Get the readable dataset from the pipeline
dataset = pipeline._dataset()
```

### Access Tables as `ReadableRelation`

You can access tables in your dataset using either attribute access or item access.

```python
# Using attribute access
items_relation = dataset.items

# Using item access
items_relation = dataset["items"]
```

## Reading Data

Once you have a `ReadableRelation`, you can read data in various formats and sizes.

### Fetch the Entire Table

**Note:** Be cautious when loading entire tables into memory, especially if they are large. Consider using limits or iterating over data in chunks.

#### As a Pandas DataFrame

```python
df = items_relation.df()
```

#### As a PyArrow Table

```python
arrow_table = items_relation.arrow()
```

#### As a List of Python Tuples

```python
items_list = items_relation.fetchall()
```

## Lazy Loading Behavior

The `ReadableDataset` and `ReadableRelation` objects are **lazy-loading**. This means that they do not immediately fetch data when you create them. Data is only retrieved when you perform an action that requires it, such as calling `.df()`, `.arrow()`, or iterating over the data. This approach optimizes performance and reduces unnecessary data loading.

## Iterating Over Data in Chunks

To handle large datasets efficiently, you can process data in smaller chunks.

### Iterate as Pandas DataFrames

```python
for df_chunk in items_relation.iter_df(chunk_size=500):
    # Process each DataFrame chunk
    pass
```

### Iterate as PyArrow Tables

```python
for arrow_chunk in items_relation.iter_arrow(chunk_size=500):
    # Process each PyArrow chunk
    pass
```

### Iterate as Lists of Tuples

```python
for items_chunk in items_relation.iter_fetch(chunk_size=500):
    # Process each chunk of tuples
    pass
```

## Modifying Queries

You can refine your data retrieval by limiting the number of records, selecting specific columns, or chaining these operations.

### Limit the Number of Records

```python
# Get the first 50 items as a PyArrow table
arrow_table = items_relation.limit(50).arrow()
```

#### Using `head()` to Get the First 5 Records

```python
df = items_relation.head().df()
```

### Select Specific Columns

```python
# Select only 'col1' and 'col2' columns
items_list = items_relation.select(["col1", "col2"]).fetchall()
```

### Chain Operations

You can combine `select`, `limit`, and other methods.

```python
# Select columns and limit the number of records
arrow_table = items_relation.select(["col1", "col2"]).limit(50).arrow()
```

## Executing Custom SQL Queries

You can execute custom SQL queries directly on the dataset.

```python
# Join 'items' and 'other_items' tables
custom_relation = dataset("SELECT * FROM items JOIN other_items ON items.id = other_items.id")
arrow_table = custom_relation.arrow()
```

**Note:** When using custom SQL queries with `dataset()`, methods like `limit` and `select` won't work. Include any filtering or column selection directly in your SQL query.

## Supported Destinations

All SQL and filesystem destinations supported by `dlt` can utilize this data access interface. For filesystem destinations, `dlt` uses **DuckDB** under the hood to create views from Parquet or JSONL files dynamically. This allows you to query data stored in files using the same interface as you would with SQL databases. If you plan on accessing data in buckets or the filesystem a lot this way, it is adviced to load data as parquet instead of jsonl, as **DuckDB** is able to only load the parts of the data actually needed for the query to work.

## Examples

### Fetch One Record as a Tuple

```python
record = items_relation.fetchone()
```

### Fetch Many Records as Tuples

```python
records = items_relation.fetchmany(chunk_size=10)
```

### Iterate Over Data with Limit and Column Selection

**Note:** When iterating over filesystem tables, the underlying DuckDB may give you a different chunksize depending on the size of the parquet files the table is based on.

```python

# dataframes
for df_chunk in items_relation.select(["col1", "col2"]).limit(100).iter_df(chunk_size=20):
    ...

# arrow tables
for arrow_table in items_relation.select(["col1", "col2"]).limit(100).iter_arrow(chunk_size=20):
    ...

# python tuples
for records in items_relation.select(["col1", "col2"]).limit(100).iter_fetch(chunk_size=20):
    # Process each modified DataFrame chunk
    ...
```

## Important Considerations

- **Memory Usage:** Loading full tables into memory without iterating or limiting can consume significant memory, potentially leading to crashes if the dataset is large. Always consider using limits or chunked iteration.

- **Lazy Evaluation:** `ReadableDataset` and `ReadableRelation` objects delay data retrieval until necessary. This design improves performance and resource utilization.

- **Custom SQL Queries:** When executing custom SQL queries, remember that additional methods like `limit()` or `select()` won't modify the query. Include all necessary clauses directly in your SQL statement.
