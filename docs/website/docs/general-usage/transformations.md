---
title: Transforming loaded datasets
description: Define SQL-based or mixed SQL + Python transformations on data that is **already** in your destination.
keywords: [transformation, dataset, sql, pipeline, ibis, arrow]
---

# Transformation

A transformation is a special type of [resource](glossary.md#resource) that takes a [dataset](glossary.md#dataset) as input. It allows to transform data ingested by `dlt`.

This snippet shows a simple SQL transformation:

<!--TODO move to snippet file-->
```python
import dlt
from dlt.sources import sql_database

# execute a pipeline to replicate a production database
ingestion_pipeline = dlt.pipeline(pipeline_name="db_replication", destination="bigquery")
ingestion_pipeline.run(sql_database(...))

# access the ingestion pipeline's dataset
ingestion_dataset = ingestion_pipeline.dataset()

# define a transformation
@dlt.transformation
def sales_by_city(dataset: dlt.Dataset):
    query = """
    SELECT
        customers.city,
        SUM(sales.amount) as total_sales
    FROM customers
    JOIN sales ON customers.id = sales.customer_id
    GROUP BY customers.city 
    """
    return dataset(query)

# execute the transformation against the ingestion dataset
transformation_pipeline = dlt.pipeline(pipeline_name="analytics", destination="bigquery")
transformation_pipeline.run(sales_by_city(ingestion_dataset))
```

dlt transformations enable you to reshape, enrich, and aggregate loaded data for downstream pipelines.

Here are a few motivating examples:
- **Layered architecture** – Combine multiple sources (e.g., Shopify, Amazon, WooCommerce) into one canonical *orders* table for unified inventory and revenue reporting.
- **RAG data processing** – Clean, chunk, and embed text data for LLM, agents, and RAG applications.
- **BI modelling** – Create a single wide table for easy querying, or use dimensional modelling to enable drag-and-drop BI. 
- **Feature engineering** – Prepare data for ML workflows (forecast, recommendation, classification).
- **Data validation** – Execute data quality checks against loaded data and store results in a validation table.

## Defining a transformation

To define a transformation, create a function that minimally:

- Uses the `@dlt.transformation` decorator. Its arguments mirror `@dlt.resource`.
- Includes a parameter of type `dlt.Dataset` in its signature.

The body of the transformation function determines the execution **location** and **timing**. This is primarily determined by:

- if `dlt.Dataset` object is used to reference or directly access data ([learn more](./dataset-access/dataset)).
- the transformation language / library used (SQL, pandas, polars, Ibis, etc) 

See the next sections for details and examples.

<!--TODO make this collapsible-->
### Location: on-backend vs. in-memory

For **on-backend** transformation, the data never leaves the destination. The compute engine of the destination is used (e.g., Snowflake, DuckDB, Databricks).

```python
# retrieve a reference to the `customers` table
dataset = pipeline.dataset()
customers = dataset.table("customers")
```

For **in-memory**, the data leaves the destination, is transformed by a Python process, and results are written to destination. 

```python
# load the `customers` table as a pandas.DataFrame or pyarrow.Table 
dataset = pipeline.dataset()
customers = dataset.table("customers")
customers_pandas = customers.df()
customers_pyarrow = customers.arrow()
```

When using in-memory execution, avoid loading the full dataset at once by[iterating over data in chunks](./dataset-access/dataset#iterating-over-data-in-chunks)

```python
dataset = pipeline.dataset()
customers = dataset.table("customers")
for chunk in customers.iter_arrow():
    ...
```

On-backend transformation is generally preferrable because it preserves native types, and avoids moving data, thus reducing latency and cost.

:::info
Related terms
- query pushdown: push some query logic to the backend
- remote vs. local execution
- server-side vs. client-side evaluation
:::

<!--TODO make this collapsible-->
### Timing: lazy vs. eager

With **lazy** execution, defining and executing the transformation are done separately. This allows to validate the transformation and infer the output schema before execution. It's a requirement for on-backend execution.

```python
sales = dataset.table("sales")
customers = dataset.table("customers")

# builds a query, no computation happens
transformation = (
    customers
    .join(sales, sales.customer_id == customers.id)
    .group_by(customers.city)
    .agg(total_sales=sales.amount.sum())
)

# returns a pandas dataframe
transformation.execute()
```

With **eager** execution, transformation statements are immediately executed and applied to the data. Consequently, dlt has limited ability to infer schemas and validate the transformation before execution

```python
sales = dataset.table("sales").df()
customers = dataset.table("customers").df()

merged_df = pd.merge(customers, sales, left_on='id', right_on='customer_id')
agg_df = merged_df.groupby('city', as_index=False)['amount'].sum()
result = agg_df.rename(columns={'amount': 'total_sales'})

# can be refactored, but still is eager
result = (
    pd.merge(customers, sales, left_on='id', right_on='customer_id')
    .groupby('city', as_index=False)['amount'].sum()
    .rename(columns={'amount': 'total_sales'})
)
```


<!--TODO refactor this to a separate page; and blog post-->
### Transformation function comparison

the next sections show the same transformation written using SQL and various Python libraries. It will highlight capabilities and trade-offs. 

#### SQL
Pass an SQL query string to the `dlt.Dataset` object to execute it.

```python
import pathlib
import dlt

@dlt.transformation
def sales_by_city(dataset: dlt.Dataset):
    query = """
    SELECT
        customers.city,
        SUM(sales.amount) as total_sales
    FROM customers
    JOIN sales ON customers.id = sales.customer_id
    GROUP BY customers.city 
    """
    return dataset(query)

# or load external .sql file
@dlt.transformation
def sales_by_city(dataset: dlt.Dataset):
    query = pathlib.Path("./queries/sales_by_city.sql").read_text()
    return dataset(query)
```
pros:
- on-backend
- lazy
- simple
- the SQL string can be loaded from a `.sql` file

cons:
- poor validation inside Python code
- SQL injection risks if using `f-strings`
- difficult to express complex queries

#### SQLGlot
Create an SQL query programmatically using [SQLGlot](https://sqlglot.com/sqlglot.html) and pass the expression to the `dlt.Dataset` object. It serves a similar goal as Ibis, but provides an SQL-like interface in Python.

```python
import dlt
from sqlglot import exp

@dlt.transformation
def sales_by_city(dataset: dlt.Dataset):
    query = (
        exp.select(
            "customers.city",
            "SUM(sales.amount) as total_sales",
        )
        .from_("customers")
        .join("sales", on="customers.id=sales.customer_id")
        .group_by("customers.city")
    )
    return dataset(query)
```

pros:
- on-backend
- lazy
- execute in-memory using DuckDB
- execute locally using DuckDB
- safely parameterize queries
- IDE support (type hinting, autocompletion, etc.)
- unit test queries
- use `if/else` statements to add clauses to query
- can mix SQGlot expressions and raw SQL string
- supports CTEs and subqueries

cons:
- more complex than SQL string
- analytical queries can become complex

#### Ibis
Create an [Ibis](https://ibis-project.org/) expression programmatically and pass it to `dlt.Dataset` object. It will convert it to SQLGlot. It serves a similar goal as SQLGlot, but provides a dataframe interface (i.e., similar to pandas).

```python
import dlt

# need to set `dataset_type="ibis"`
dataset = dlt.pipeline(...).dataset(dataset_type="ibis")

@dlt.transformation
def sales_by_city(dataset: dlt.Dataset):
    # lazy reference to tables
    customers = dataset.table("customers")
    sales = dataset.table("sales")

    query = (
        customers
        .join(sales, sales.customer_id == customers.id)
        .group_by(customers.city)
        .agg(total_sales=sales.amount.sum())
    )
    return dataset(query)
```

pros:
- on-backend
- lazy
- execute in-memory using DuckDB
- execute locally using DuckDB
- safely parameterize queries
- IDE support (type hinting, autocompletion, etc.)
- unit test queries
- use `if/else` statements to add clauses to query
- can mix Ibis expressions and raw SQL via [.sql()](https://ibis-project.org/reference/expression-tables#ibis.expr.types.relations.Table.sql)
- suited for complex analytical queries (e.g.., self-reference using `ibis._`)

cons:
- learning curve; it departs from pandas and SQL
- analytical queries can become complex

#### Pandas
Load data into memory using `dlt.Dataset` object, execute [pandas](https://pandas.pydata.org/) transformations, and return a `pandas.DataFrame`. pandas is a reasonable option for small datasets.

```python
import pandas as pd

@dlt.transformation
def sales_by_city(dataset: dlt.Dataset):
    # eager access to tables
    sales = dataset.table("sales").df()
    customers = dataset.table("customers").df()

    result = (
        pd.merge(customers, sales, left_on='id', right_on='customer_id')
        .groupby('city', as_index=False)['amount'].sum()
        .rename(columns={'amount': 'total_sales'})
    )
    return result
```

pros:
- well-known and lots of integrations
- use `if/else` statements to add clauses to query

cons:
- in-memory
- eager
- converting data to a `pandas.DataFrame` can be expensive
- less performant than in-memory alternatives
- automated type coercion


#### Pyarrow
Load data into memory using `dlt.Dataset` object, execute [pyarrow](https://arrow.apache.org/docs/python/compute.html) transformations, and return a `pyarrow.Table`.

```python
import pyarrow.compute as pc

@dlt.transformation
def sales_by_city(dataset: dlt.Dataset):
    # eager access to tables
    sales = dataset.table("sales").arrow()
    customers = dataset.table("customers").arrow()

    result = (
        customers.join(sales, keys=("id", "customer_id"), join_type="inner")
        .group_by("city")
        .aggregate([("amount", "sum")])
        .rename_columns(["city", "total_sales"])
    )
    return result
```

pros:
- no conversion overhead between read, transform, write
- preserves pyarrow types
- performant vectorized operations
- strict typing; will fail eagerly on type-mismatch

cons:
- in-memory
- eager
- relatively unknown API
- limited set of operations


#### Polars
There are many ways to use [Polars](https://docs.pola.rs/): eager, lazy, streaming. While Polars uses in-memory execution, see [Narwhals LazyFrame](#lazyframe-like) for on-backend execution.

##### Eager DataFrame
Load data into memory using `dlt.Dataset` object, convert `pyarrow.Table` to `polars.DataFrame`, transform, and return a dataframe.

```python
import polars as pl

@dlt.transformation
def sales_by_city(dataset: dlt.Dataset):
    # eager access to tables
    # `from_arrow()` should be a zero copy operation in most cases
    sales = pl.from_arrow(dataset.table("sales").arrow())
    customers = pl.from_arrow(dataset.table("customers").arrow())

    result = (
        customers.join(sales, left_on="id", right_on="customer_id")
        .group_by("city")
        .agg(pl.col("amount").sum().alias("total_sales"))
    )
    return result
```

pros:
- performant in-memory operations (memory efficient, multi-threaded, out-of-core)
- strict typing
- well-known and lots of integrations

cons:
- in-memory
- eager

##### LazyFrame
Load data into memory using `dlt.Dataset` object, convert `pyarrow.Table` to `polars.LazyFrame`, transform, and return a dataframe. It is one of the few option that's lazy and in-memory.

:::caution
🚧 `polars.LazyFrame` support is on the roadmap and not currently implemented. Interested? Reach out to us on Slack or GitHub.
:::

```python
import polars as pl

@dlt.transformation
def sales_by_city(dataset: dlt.Dataset):
    # eager access to tables
    # `from_arrow()` should be a zero copy operation in most cases
    sales = pl.from_arrow(dataset.table("sales").arrow()).lazy()
    customers = pl.from_arrow(dataset.table("customers").arrow()).lazy()

    query = (
        customers.join(sales, left_on="id", right_on="customer_id")
        .group_by("city")
        .agg(pl.col("amount").sum().alias("total_sales"))
    )
    # call `query.collect()` to get results
    return query
```

pros:
- lazy

cons:
- in-memory
- not all operations can be lazy

##### Streaming
:::caution
🚧 Streaming is still [in early development](https://docs.pola.rs/user-guide/concepts/_streaming/). We plan to support once the feature matures. Interested? Reach out to us on Slack or GitHub.
:::

#### Narwhals
[Narwhals](https://github.com/narwhals-dev/narwhals) is a library that allows you to write Polars code, eager or lazy, and execute on other backends.

:::caution
🚧 Narwhals support is on the roadmap and not currently implemented. Interested? Reach out to us on Slack or GitHub.
:::

##### DataFrame-like
The Narwhals code is exactly the same as the Polars code (with import `pl` replaced by `nw`), but it will be executed on the pyarrow engine. 

```python
import narwhals as nw

@dlt.transformation
def sales_by_city(dataset: dlt.Dataset):
    # eager access to tables
    # calling `.from_native()` on `pyarrow.Table` will use the pyarrow backend
    sales = nw.from_native(dataset.table("sales").arrow())
    customers = nw.from_native(dataset.table("customers").arrow())

    result = (
        customers.join(sales, left_on="id", right_on="customer_id")
        .group_by("city")
        .agg(pl.col("amount").sum().alias("total_sales"))
    )
    # use `.to_native()` to retrieve a `pyarrow.Table`
    return result.to_native()
```

Pros:
- GPU acceleration via [cuDF](https://docs.rapids.ai/api/cudf/stable/) backend
- distributed execution via [Modin](https://github.com/modin-project/modin) backend

Cons:
- eager
- in-memory
- See [API completeness](https://narwhals-dev.github.io/narwhals/api-completeness/)

##### LazyFrame-like
By using `.from_native()` on an Ibis table, we'll be able to use to execute the polars expression on the destination backend. This uniquely allow to execute Polars code on-backend, as SQL. This is equivalent in features to Ibis and SQLGlot.

```python
import narwhals as nw

@dlt.transformation
def sales_by_city(dataset: dlt.Dataset):
    # eager access to tables
    # calling `.from_native()` on a lazy `ibis.Table` will use the destination backend
    sales = nw.from_native(dataset.table("sales")._ibis_object)
    customers = nw.from_native(dataset.table("customers")._ibis_object)

    result = (
        customers.join(sales, left_on="id", right_on="customer_id")
        .group_by("city")
        .agg(pl.col("amount").sum().alias("total_sales"))
    )
    # use `.to_native()` to retrieve a lazy `ibis.Table`
    return result.to_native()
```

Pros:
- lazy
- on-backend via Ibis backend
- distributed execution via PySpark backend (this should be equivalent to PySpark on Ibis)
- multimodal support via [Daft](https://github.com/Eventual-Inc/Daft) backend (WIP)
- native Snowpark execution via Snowpark backend ([WIP](https://github.com/narwhals-dev/narwhals/issues/1419))

Cons:
- in-memory 
- See [API completeness](https://narwhals-dev.github.io/narwhals/api-completeness/)


## dlt transformation engine

<!--TODO-->

## Quickstart

> The snippets below use a simple "fruit-shop" source. You can copy–paste everything into one script and run it.

### 1. Load some example data

The snippets below assume that we have a simple fruitshop dataset as produced by the dlt fruitshop template:

<!--@@@DLT_SNIPPET ./transformation-snippets.py::quick_start_example-->


### 1.1 Use the fruitshop template as a starting point

Alternatively, you can follow the code examples below by creating a new pipeline with the fruitshop template and running transformations on the resulting dataset:

```sh
dlt init fruitshop duckdb
```

### 2. Inspect the dataset

<!--@@@DLT_SNIPPET ./transformation-snippets.py::dataset_inspection-->

### 3. Write and run a transformation based on ibis expressions

<!--@@@DLT_SNIPPET ./transformation-snippets.py::basic_transformation-->


### 3.1 Alternatively use pure SQL for the transformation 

<!--@@@DLT_SNIPPET ./transformation-snippets.py::sql_queries_short-->

That's it — `copied_customers` is now a new table in **the same** DuckDB schema with the first 5 customers when ordered by name. `dlt` has detected that we are loading into the same dataset
and executed this transformation in SQL - no data was transferred to and from the machine executing this pipeline. Additionally, the new destination table `copied_customers` was automatically evolved
to the correct new schema, and you could also set a different write disposition and even merge data from a transformation.

## Loading to other datasets


### Loading to another dataset on the same physical location

Below we load to the same DuckDB instance with a new pipeline that points to another `dataset`. dlt will be able to detect that both datasets live on the same destination and
will run the transformation as pure SQL.

<!--@@@DLT_SNIPPET ./transformation-snippets.py::loading_to_other_datasets-->

### Loading to another dataset on a different physical location

Below we load the data from our local DuckDB instance to a Postgres instance. dlt will use the query to extract the data as Parquet files and will do a regular dlt load, pushing the data
to Postgres. Note that you can use the exact same transformation functions for both scenarios.

<!--@@@DLT_SNIPPET ./transformation-snippets.py::loading_to_other_datasets_other_engine-->

## Grouping multiple transformations in a source

`dlt transformations` can be grouped like all other resources into sources and will be executed together. You can even mix regular resources and transformations in one pipeline load.

<!--@@@DLT_SNIPPET ./transformation-snippets.py::multiple_transformations-->

## Writing your queries in SQL

If you prefer to write your queries in SQL, you can omit ibis expressions by simply creating a TReadableRelation from a query on your dataset:

<!--@@@DLT_SNIPPET ./transformation-snippets.py::sql_queries-->


## Using pandas dataframes or arrow tables

You can also directly write transformations using pandas or arrow. Please note that your transformation resource will act like a regular resource in this case, you will not have column level hints forward but rather dlt will just see the dataframes or arrow tables you yield and process them like from any other resource. This may change in the future.

<!--@@@DLT_SNIPPET ./transformation-snippets.py::arrow_dataframe_operations-->


## Schema evolution and hints lineage

When executing transformations, dlt computes the resulting schema before the transformation is executed. This allows dlt to:

1. Migrate the destination schema accordingly, creating new columns or tables as needed
2. Fail early if there are schema mismatches that cannot be resolved
3. Preserve column-level hints from source to destination

### Schema evolution

For example, if your transformation joins two tables and creates new columns, dlt will automatically update the destination schema to accommodate these changes. If your transformation would result in incompatible schema changes (like changing a column's data type in a way that could lose data), dlt will fail before executing the transformation, protecting your data and saving execution and debug time. 

You can inspect the computed result schema during development by looking at the result of `compute_columns_schema` on your `TReadableRelation`:

<!--@@@DLT_SNIPPET ./transformation-snippets.py::computed_schema-->

### Column level hint forwarding

When creating or updating tables with transformation resources, dlt will also forward certain column hints to the new tables. In our fruitshop source, we have applied a custom hint named
`x-pii` set to True for the `name` column, which indicates that this column contains PII (personally identifiable information). We might want to know downstream of our transformation layer
which columns resulted from origin columns that contain private data:

<!--@@@DLT_SNIPPET ./transformation-snippets.py::column_level_lineage-->

Features and limitations:

* `dlt` will only forward certain types of hints to the resulting tables: custom hints starting with `x-...` and type hints such as `nullable`, `precision`, `scale`, and `timezone`. Other hints, such as `primary_key` or `merge_keys`, will need to be set by the developer, since dlt does not know how the transformed tables will be used.
* `dlt` will not be able to forward hints for columns that are the result of combining two origin columns, for example by concatenating them or similar operations.

## Normalization

When executing transformations, dlt will add internal dlt columns to your SQL queries depending on the configuration:

- `_dlt_load_id`, which tracks which load operation created or modified each row, is **added by default**. Even if present in your query, the `_dlt_load_id` column will be **replaced with a constant value** corresponding to the current load ID. To disable this behavior, set:
    ```toml
    [normalize.model_normalizer]
    add_dlt_load_id = false
    ```
    In this case, the column will not be added or replaced.

- `_dlt_id`, a unique identifier for each row, is **not added by default**. If your query already includes a `_dlt_id` column, it will be left unchanged. To enable automatic generation of this column when it’s missing, set:
    ```toml
    [normalize.model_normalizer]
    add_dlt_id = true
    ```
    When enabled and the column is not in the query, dlt will generate a `_dlt_id`. Note that if the column is already present, it will **not** be replaced.

    The `_dlt_id` column is generated using the destination's UUID function, such as `generateUUIDv4()` in ClickHouse. For dialects without native UUID support:
     - In **Redshift**, `_dlt_id` is generated using an `MD5` hash of the load ID and row number.
     - In **SQLite**, `_dlt_id` is simulated using `lower(hex(randomblob(16)))`.

Additionally, column names are normalized according to the naming schema selected and the identifier capabilities of the destinations. This ensures compatibility and consistent naming conventions across different data sources and destination systems.

This allows dlt to maintain data lineage and enables features like incremental loading and merging, even when working with raw SQL queries.

:::info
The normalization described here, including automatic injection or replacement of dlt columns, applies only to SQL-based transformations. Python-based transformations, such as those using dataframes or arrow tables, follow the [regular normalization process](../../reference/explainers/how-dlt-works#normalize).
:::

## Local in-transit transformations example

If you require aggregated or otherwise transformed data in your warehouse, but would like to avoid or reduce the costs of running queries across many rows in your warehouse tables, you can run some or all of your transformations "in transit" while loading data from your source. The code below demonstrates how you can extract data with our `rest_api` source to a local DuckDB instance and then forward aggregated data to a warehouse destination.

<!--@@@DLT_SNIPPET ./transformation-snippets.py::in_transit_transformations-->

This script demonstrates:
- Fetching data from a REST API using dlt's rest_api_source
- Loading raw data into a local DuckDB instance as an intermediate step
- Transforming the data by joining orders with stores and aggregating order counts directly on the local DuckDB instance, not in the destination warehouse
- Loading only the aggregated results to a production warehouse (Postgres)
- Reducing warehouse compute costs by performing transformations locally in DuckDB
- Using multiple pipelines in a single workflow for different stages of processing

## Incremental transformations example

:::info
This example shows how to do incremental transformations based on an incremental primary key of the original tables. We are working on simplifying incremental transformations at the moment.
:::

<!--@@@DLT_SNIPPET ./transformation-snippets.py::incremental_transformations-->

This example demonstrates how you can incrementally transform incoming new data for the customers table into a cleaned_customers table where the name column has been removed. It:
 * Uses primary key-based incremental loading to process only new data
 * Tracks the highest ID processed so far to filter out already processed records
 * Handles first-time runs with the PipelineNeverRan exception
 * Removes sensitive data (name column) during the transformation
 * Uses write_disposition="append" to add only new records to the destination table
 * Can be run repeatedly as new data arrives, processing only the delta



