---
title: Access datasets in Python
description: Conveniently access the data loaded to any destination in Python
keywords: [destination, schema, data, access, retrieval]
---

# Access loaded data in Python

This guide explains how to access and manipulate data that has been loaded into your destination using the `dlt` Python library. After running your pipelines and loading data, you can use the `pipeline.dataset()` and data frame expressions, Ibis or SQL to query the data and read it as records, Pandas frames or Arrow tables.

## Quick start example

Here's a full example of how to retrieve data from a pipeline and load it into a Pandas DataFrame or a PyArrow Table.

```py
# Assuming you have a Pipeline object named 'pipeline'. You can create one with the dlt cli: dlt init fruitshop duckdb
# and you have loaded the data of the fruitshop example source into the destination
# the tables available in the destination are:
# - customers
# - inventory
# - purchases

# Step 1: Get the readable dataset from the pipeline
dataset = pipeline.dataset()

# Step 2: Access a table as a ReadableRelation
customers_relation = dataset.customers  # Or dataset["customers"]

# Step 3: Fetch the entire table as a Pandas DataFrame
df = customers_relation.df()  # or customers_relation.df(chunk_size=50)

# Alternatively, fetch as a PyArrow Table
arrow_table = customers_relation.arrow()
```

## Getting started

Assuming you have a `Pipeline` object (let's call it `pipeline`), you can obtain a `Dataset` which is contains the crendentials and schema to your destination dataset. You can run construct a query and execute it on the dataset to retrieve a `Relation` which you may use to retrieve data from the `Dataset`.

**Note:** The `Dataset` and `Relation` objects are **lazy-loading**. They will only query and retrieve data when you perform an action that requires it, such as fetching data into a DataFrame or iterating over the data. This means that simply creating these objects does not load data into memory, making your code more efficient.


### Access the dataset

```py
# Get the readable dataset from the pipeline
dataset = pipeline.dataset()

# print the row counts of all tables in the destination as dataframe
print(dataset.row_counts().df())
```

### Access tables as dataset

The simplest way of getting a Relation from a Dataset is to get a full table relation:

```py
# Using attribute access
customers_relation = dataset.customers

# Using item access
customers_relation = dataset["customers"]
```

### Creating relations with sql query strings

```py
# Join 'customers' and 'purchases' tables and filter by quantity
query = """
SELECT *  
    FROM customers 
JOIN purchases 
    ON customers.id = purchases.customer_id
WHERE purchases.quantity > 1
"""
joined_relation = dataset(query)
```

## Reading data

Once you have a `Relation`, you can read data in various formats and sizes.

### Fetch the entire table

:::warning
Loading full tables into memory without limiting or iterating over them can consume a large amount of memory and may cause your program to crash if the table is too large. It's recommended to use chunked iteration or apply limits when dealing with large datasets. 
:::

#### As a Pandas DataFrame

```py
df = customers_relation.df()
```

#### As a PyArrow Table

```py
arrow_table = customers_relation.arrow()
```

#### As a list of Python tuples

```py
items_list = customers_relation.fetchall()
```

## Lazy loading behavior

The `Dataset` and `Relation` objects are **lazy-loading**. This means that they do not immediately fetch data when you create them. Data is only retrieved when you perform an action that requires it, such as calling `.df()`, `.arrow()`, or iterating over the data. This approach optimizes performance and reduces unnecessary data loading.

## Iterating over data in chunks

To handle large datasets efficiently, you can process data in smaller chunks.

### Iterate as Pandas DataFrames

```py
for df_chunk in customers_relation.iter_df(chunk_size=5):
    # Process each DataFrame chunk
    pass
```

### Iterate as PyArrow Tables

```py
for arrow_chunk in customers_relation.iter_arrow(chunk_size=5):
    # Process each PyArrow chunk
    pass
```

### Iterate as lists of tuples

```py
for items_chunk in customers_relation.iter_fetch(chunk_size=5):
    # Process each chunk of tuples
    pass
```

The methods available on the Relation correspond to the methods available on the cursor returned by the SQL client. Please refer to the [SQL client](./sql-client.md#supported-methods-on-the-cursor) guide for more information.

## Connection Handling

For every call that actually fetches data from the destination, such as `df()`, `arrow()`, `fetchall()` etc., the dataset will open a connection and close it after it has been retrieved or the iterator is completed. You can keep the connection open for multiple requests with the dataset context manager:

```py
# the dataset context manager will keep the connection open
# and close it after the with block is exited
with dataset as dataset_:
    print(dataset.customers.limit(50).arrow())
    print(dataset.purchases.arrow())
```

## Special queries

You can use the `row_counts` method to get the row counts of all tables in the destination as a DataFrame.

```py
# print the row counts of all tables in the destination as dataframe
print(dataset.row_counts().df())

# or as tuples
print(dataset.row_counts().fetchall())
```

## Modifying queries

You can refine your data retrieval by limiting the number of records, selecting specific columns, sorting the results, filtering rows, aggregating minimum and maximum values on a specific column, or chaining these operations.

### Limit the number of records

```py
# Get the first 50 items as a PyArrow table
arrow_table = customers_relation.limit(50).arrow()
```

#### Using `head()` to get the first 5 records

```py
df = customers_relation.head().df()
```

### Select specific columns

```py
# Select only 'id' and 'name' columns
items_list = customers_relation.select("id", "name").fetchall()

# Alternate notation with brackets
items_list = customers_relation[["id", "name"]].fetchall()

# Only get one column
items_list = customers_relation[["name"]].fetchall()
```

### Sort results

```py
# Order by 'id'
ordered_list = customers_relation.order_by("id").fetchall()
```

### Filter rows

```py
# Filter by 'id'
filtered = customers_relation.where("id", "in", [3, 1, 7]).fetchall()

# Filter with raw SQL string
filtered = customers_relation.where("id = 1").fetchall()

# Filter with sqlglot expression
import sqlglot.expressions as sge

expr = sge.EQ(
    this=sge.Column(this=sge.to_identifier("id", quoted=True)),
    expression=sge.Literal.number("7"),
)
filtered = customers_relation.where(expr).fetchall()
```

### Aggregate data

```py
# Get max 'id'
max_id = customers_relation.select("id").max().fetchscalar()

# Get min 'id'
min_id = customers_relation.select("id").min().fetchscalar()
```

### Chain operations

You can combine `select`, `limit`, and other methods.

```py
# Select columns and limit the number of records
arrow_table = customers_relation.select("id", "name").limit(50).arrow()
```

## Modifying queries with ibis expressions

If you install the amazing [ibis](https://ibis-project.org/) library, you can use ibis expressions to modify your queries.

```sh
pip install ibis-framework
```

dlt will then allow you to get an `ibis.UnboundTable` for each table which you can use to build a query with ibis expressions, which you can then execute on your dataset.

:::warning
A previous version of dlt allowed to use ibis expressions in a slightly different way, allowing users to directly execute and retrieve data on ibis Unbound tables. This method does not work anymore. See the migration guide below for instructions on how to update your code.
:::

```py
# now that ibis is installed, we can get ibis unbound tables from the dataset
dataset = pipeline.dataset()

# get two table expressions
customers_expression = dataset.table("customers", table_type="ibis")
purchases_expression = dataset.table("purchases", table_type="ibis")

# join them using an ibis expression
join_expression = customers_expression.join(
    purchases_expression, customers_expression.id == purchases_expression.customer_id
)

# now we can use the ibis expression to filter the data
filtered_expression = join_expression.filter(purchases_expression.quantity > 1)

# we can pass the expression back to the dataset to get a relation that can be executed
relation = dataset(filtered_expression)
# and we can inspect the query that will be used to read the data
print(relation)

# and finally fetch the data as a pandas dataframe, the same way we would do with a normal relation
print(relation.df())

# a few more examples

# get all customers from berlin and london and load them as a dataframe
expr = customers_expression.filter(customers_expression.city.isin(["berlin", "london"]))
print(dataset(expr).df())

# limit and offset, then load as an arrow table
expr = customers_expression.limit(10, offset=5)
print(dataset(expr).arrow())

# mutate columns by adding a new colums that always is 10 times the value of the id column
expr = customers_expression.mutate(new_id=customers_expression.id * 10)
print(dataset(expr).df())

# sort asc and desc
import ibis

expr = customers_expression.order_by(ibis.desc("id"), ibis.asc("city")).limit(10)
print(dataset(expr).df())

# group by and aggregate
expr = (
    customers_expression.group_by("city")
    .having(customers_expression.count() >= 3)
    .aggregate(sum_id=customers_expression.id.sum())
)
print(dataset(expr).df())

# subqueries
expr = customers_expression.filter(customers_expression.city.isin(["berlin", "london"]))
print(dataset(expr).df())
```

You can learn more about the available expressions on the [ibis for sql users](https://ibis-project.org/tutorials/ibis-for-sql-users) page. 


### Migrating from the previous dlt / ibis implementation

As describe above, the new way to use ibis expressions is to first get one or many `UnboundTable` objects, construct your expression and then bind it to your data via the `Dataset` to get a `Relation` object which you may execute to retrieve your data.

An example from our previous docs for joining a customers and a purchase table was this:

```py
# get two relations
customers_relation = dataset["customers"]
purchases_relation = dataset["purchases"]

# join them using an ibis expression
joined_relation = customers_relation.join(
    purchases_relation, customers_relation.id == purchases_relation.customer_id
)

# ... do other ibis operations

# directly fetch the data on the expression we have built
df = joined_relation.df()
```

The migrated version looks like this:

```py
# we need to explicitely select table type ibis here
customers_expression = dataset.table("customers", table_type="ibis")
purchases_expression = dataset.table("purchases", table_type="ibis")

# join them using an ibis expression, same code as above
joined_epxression = customers_expression.join(
    purchases_expression, customers_expression.id == purchases_expression.customer_id
)

# ... do other ibis operations, would be same as before

# now convert the expression to a relation
joined_relation = dataset(joined_epxression)

# execute as before
df = joined_relation.df()
```


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

```py
record = customers_relation.fetchone()
```

### Fetch many records as tuples

```py
records = customers_relation.fetchmany(10)
```

### Iterate over data with limit and column selection

**Note:** When iterating over filesystem tables, the underlying DuckDB may give you a different chunk size depending on the size of the parquet files the table is based on.

```py
# Dataframes
for df_chunk in customers_relation.select("id", "name").limit(100).iter_df(chunk_size=20): ...

# Arrow tables
for arrow_table in (
    customers_relation.select("id", "name").limit(100).iter_arrow(chunk_size=20)
): ...

# Python tuples
for records in customers_relation.select("id", "name").limit(100).iter_fetch(chunk_size=20):
    # Process each modified DataFrame chunk
    ...
```

## Advanced usage

### Loading a `Relation` into a pipeline table

Since the `iter_arrow` and `iter_df` methods are generators that iterate over the full `Relation` in chunks, you can use them as a resource for another (or even the same) `dlt` pipeline:

```py
# Create a readable relation with a limit of 1m rows
limited_customers_relation = dataset.customers.limit(1_000_000)

# Create a new pipeline
other_pipeline = dlt.pipeline(pipeline_name="other_pipeline", destination="duckdb")

# We can now load these 1m rows into this pipeline in 10k chunks
other_pipeline.run(
    limited_customers_relation.iter_arrow(chunk_size=10_000), table_name="limited_customers"
)
```

Learn more about [transforming data in Python with Arrow tables or DataFrames](../../dlt-ecosystem/transformations/python).

### Using `ibis` to query data

Visit the [Native Ibis integration](./ibis-backend.md) guide to learn more.

## Important considerations

- **Memory usage:** Loading full tables into memory without iterating or limiting can consume significant memory, potentially leading to crashes if the dataset is large. Always consider using limits or chunked iteration.

- **Lazy evaluation:** `Dataset` and `Relation` objects delay data retrieval until necessary. This design improves performance and resource utilization.

- **Custom SQL queries:** When executing custom SQL queries, remember that additional methods like `limit()` or `select()` won't modify the query. Include all necessary clauses directly in your SQL statement.

