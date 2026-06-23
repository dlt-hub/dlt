---
title: Transform data in Python with Arrow tables or DataFrames
description: Transforming data loaded by a dlt pipeline with Pandas or Polars DataFrames and Arrow tables
keywords: [transform, pandas, polars, arrow]
---

# Transform data in Python with Arrow tables or DataFrames

You can transform your data in Python using Pandas DataFrames, Arrow tables, or Polars DataFrames. To get started, please read the [dataset docs](../../general-usage/dataset-access/dataset).


## Interactively transforming your data in Python

Using the methods explained in the [dataset docs](../../general-usage/dataset-access/dataset), you can fetch data from your destination into a DataFrame or Arrow table in your local Python process and work with it interactively. This even works for filesystem destinations:


The example below reads GitHub reactions data from the `issues` table and
counts the reaction types.

```py
pipeline = dlt.pipeline(
    pipeline_name="github_pipeline",
    destination="duckdb",
    dataset_name="github_reactions",
    dev_mode=True
)

# get a data frame of all reactions from the dataset
github_issues = pipeline.dataset().table("issues")
reactions = github_issues.select(
    "reactions__+1", "reactions__-1", "reactions__laugh", "reactions__hooray", "reactions__rocket"
).df()

# calculate and print out the sum of all reactions
counts = reactions.sum(0).sort_values(0, ascending=False)
print(counts)

# alternatively, you can fetch the data as an arrow table
reactions = github_issues.select(
    "reactions__+1", "reactions__-1", "reactions__laugh", "reactions__hooray", "reactions__rocket"
).arrow()
# ... do transformations on the arrow table
```

### Using Ibis

Ibis is a powerful portable Python dataframe library. Learn more about what it is and how to use it in the [official documentation](https://ibis-project.org/).

`dlt` provides an easy way to hand over your loaded dataset to an Ibis backend connection. The returned object is a native Ibis connection to the destination, which you can use to read and even transform data. Note that the Ibis expression language is querying-first: you can materialize query results into new tables (e.g. with `create_table`) but for row-level DML you should use the [`dlt` SQL client](sql.md).

:::tip
Not all destinations supported by `dlt` have an equivalent Ibis backend. Natively supported destinations include DuckDB (including Motherduck), Postgres (Redshift is supported via the Postgres backend for Ibis versions lower than 10.4.0), Snowflake, Clickhouse, MSSQL (including Synapse), and BigQuery. The filesystem destination is supported via the [Filesystem SQL client](sql.md#the-filesystem-sql-client); please install the DuckDB backend for Ibis to use it. Mutating data with Ibis on the filesystem will not result in any actual changes to the persisted files.
:::

To use the Ibis backend, you will need to have the `ibis-framework` package with the correct Ibis extra installed. The following example will install the DuckDB backend:

```sh
pip install ibis-framework[duckdb]
```

`dlt` datasets have a helper method to return an Ibis connection to the destination they live on:

```py
# get the dataset from the pipeline
dataset = pipeline.dataset()
dataset_name = pipeline.dataset_name

# get the native ibis connection from the dataset
ibis_connection = dataset.ibis()

# list all tables in the dataset
# NOTE: You need to provide the dataset name to ibis, in ibis datasets are named databases
print(ibis_connection.list_tables(database=dataset_name))

# get the items table
table = ibis_connection.table("items", database=dataset_name)

# print the first 10 rows
print(table.limit(10).execute())

# Visit the ibis docs to learn more about the available methods
```

:::caution Breaking change in dlt 1.25.0
`dataset.ibis()` now passes all schemas from the dataset to the Ibis backend. On filesystem destinations, this means Ibis will see tables from every schema in the dataset and not just the default one. If two schemas define the same table name, the Ibis table will contain rows from both schemas combined. To get the previous single-schema behavior, create the dataset with an explicit schema: `pipeline.dataset(schema="my_schema").ibis()`.
:::

## Persisting your transformed data

Since dlt supports Arrow tables, Pandas or Polars DataFrames from resources directly, you can use the same pipeline to load the transformed data back into the destination.


### A simple example

A simple example that creates a new table from an existing user table but only with columns that do not contain private information. Note that we use the `iter_arrow()` method on the relation to iterate over the arrow table instead of fetching it all at once.

```py
pipeline = dlt.pipeline(
    pipeline_name="users_pipeline",
    destination="duckdb",
    dataset_name="users_raw",
    dev_mode=True
)

# get user relation with only a few columns selected, but omitting email and name
users = pipeline.dataset().table("users").select("age", "amount_spent", "country")

# load the data into a new table called users_clean in the same dataset
pipeline.run(users.iter_arrow(chunk_size=1000), table_name="users_clean")
```

### A more complex example

The example above could easily be done in SQL. Let's assume you'd like to actually do in Python some Arrow transformations. For this we will create a resource from which we can yield the modified Arrow tables. The same is possibly with DataFrames.

```py
import pyarrow.compute as pc

pipeline = dlt.pipeline(
    pipeline_name="users_pipeline",
    destination="duckdb",
    dataset_name="users_raw",
    dev_mode=True
)

# NOTE: this resource will work like a regular resource and support write_disposition, primary_key, etc.
# NOTE: For selecting only users above 18, we could also use the filter method on the relation with ibis expressions
@dlt.resource(table_name="users_clean")
def users_clean():
    users = pipeline.dataset().table("users")
    for arrow_table in users.iter_arrow(chunk_size=1000):

        # we want to filter out users under 18
        age_filter = pc.greater_equal(arrow_table["age"], 18)
        arrow_table = arrow_table.filter(age_filter)

        # we want to hash the email column
        arrow_table = arrow_table.append_column("email_hash", pc.sha256(arrow_table["email"]))

        # we want to remove the email column and name column
        arrow_table = arrow_table.drop(["email", "name"])

        # yield the transformed arrow table
        yield arrow_table


pipeline.run(users_clean())
```

### A Polars example

You can also use Polars for transformations. Polars DataFrames and LazyFrames are automatically converted to Arrow tables when yielded from a resource.

```py
import polars as pl

pipeline = dlt.pipeline(
    pipeline_name="users_pipeline",
    destination="duckdb",
    dataset_name="users_raw",
    dev_mode=True
)

@dlt.resource(table_name="users_clean")
def users_clean():
    users = pipeline.dataset().table("users")
    for arrow_table in users.iter_arrow(chunk_size=1000):
        # convert to Polars for transformation
        df = pl.from_arrow(arrow_table)

        # filter out users under 18
        df = df.filter(pl.col("age") >= 18)

        # drop sensitive columns
        df = df.drop(["email", "name"])

        # yield the Polars DataFrame directly; dlt converts it to Arrow
        yield df


pipeline.run(users_clean())
```

## Other transforming tools

If you want to transform your data before loading, you can use Python. If you want to transform the
data after loading, you can use Pandas or one of the following:

1. [dbt.](dbt/dbt.md) (recommended)
2. [`dlt` SQL client.](sql.md)
