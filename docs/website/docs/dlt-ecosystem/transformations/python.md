---
title: Transforming data in Python with Arrow tables or DataFrames
description: Transforming data loaded by a dlt pipeline with pandas dataframes or arrow tables
keywords: [transform, pandas]
---

# Transforming data in Python with Arrow tables or DataFrames

You can transform your data in Python using Pandas DataFrames or Arrow tables. To get started, please read the [dataset docs](../../general-usage/dataset-access/dataset).


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

# get a dataframe of all reactions from the dataset
reactions = pipeline.dataset().issues.select("reactions__+1", "reactions__-1", "reactions__laugh", "reactions__hooray", "reactions__rocket").df()

# calculate and print out the sum of all reactions
counts = reactions.sum(0).sort_values(0, ascending=False)
print(counts)

# alternatively, you can fetch the data as an arrow table
reactions = pipeline.dataset().issues.select("reactions__+1", "reactions__-1", "reactions__laugh", "reactions__hooray", "reactions__rocket").arrow()
# ... do transformations on the arrow table
```

## Persisting your transformed data

Since dlt supports DataFrames and Arrow tables from resources directly, you can use the same pipeline to load the transformed data back into the destination.


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
users = pipeline.dataset().users.select("age", "amount_spent", "country")

# load the data into a new table called users_clean in the same dataset
pipeline.run(users.iter_arrow(chunk_size=1000), table_name="users_clean")
```

### A more complex example

The example above could easily be done in SQL. Let's assume you'd like to actually do in Python some Arrow transformations. For this will create a resources from which we can yield the modified Arrow tables. The same is possibly with DataFrames.

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
    users = pipeline.dataset().users
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

## Other transforming tools

If you want to transform your data before loading, you can use Python. If you want to transform the
data after loading, you can use Pandas or one of the following:

1. [dbt.](dbt/dbt.md) (recommended)
2. [`dlt` SQL client.](sql.md)

