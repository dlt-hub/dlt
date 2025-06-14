---
title: Transformation libraries
description: Comparison of transformation libraries compatible with `@dlt.transformation`
keywords: [transformation, dataset, sql, pipeline, ibis, pyarrow, polars, narwhals, pandas, sqlglot]
---

This page reviews different libraries compatible with `@dlt.transformation`. The same transformation is rewritten with each tool to give you a sense of the syntax. Then, capabilities and trade-offs are discussed. 

## SQL
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

## SQLGlot
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

## Ibis
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

## Pandas
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


## Pyarrow
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


## Polars
There are many ways to use [Polars](https://docs.pola.rs/): eager, lazy, streaming. While Polars uses in-memory execution, see [Narwhals LazyFrame](#lazyframe-like) for on-backend execution.

### Eager DataFrame
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

### LazyFrame
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

### Streaming
:::caution
🚧 Streaming is still [in early development](https://docs.pola.rs/user-guide/concepts/_streaming/). We plan to support once the feature matures. Interested? Reach out to us on Slack or GitHub.
:::

## Narwhals
[Narwhals](https://github.com/narwhals-dev/narwhals) is a library that allows you to write Polars code, eager or lazy, and execute on other backends.

:::caution
🚧 Narwhals support is on the roadmap and not currently implemented. Interested? Reach out to us on Slack or GitHub.
:::

### DataFrame-like
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

### LazyFrame-like
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
- native [Snowpark](https://www.snowflake.com/en/product/features/snowpark/) execution via Snowpark backend ([WIP](https://github.com/narwhals-dev/narwhals/issues/1419))

Cons:
- in-memory 
- See [API completeness](https://narwhals-dev.github.io/narwhals/api-completeness/)

