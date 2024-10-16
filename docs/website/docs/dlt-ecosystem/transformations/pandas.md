---
title: Transform the data with Pandas
description: Transform the data loaded by a dlt pipeline with Pandas
keywords: [transform, pandas]
---

# Transform the data with Pandas

You can fetch the results of any SQL query as a dataframe. If the destination supports that
natively (i.e., BigQuery and DuckDB), `dlt` uses the native method. Thanks to this, reading
dataframes can be really fast! The example below reads GitHub reactions data from the `issues` table and
counts the reaction types.

```py
pipeline = dlt.pipeline(
    pipeline_name="github_pipeline",
    destination="duckdb",
    dataset_name="github_reactions",
    dev_mode=True
)
with pipeline.sql_client() as client:
    with client.execute_query(
        'SELECT "reactions__+1", "reactions__-1", reactions__laugh, reactions__hooray, reactions__rocket FROM issues'
    ) as table:
        # calling `df` on a cursor, returns the data as a pandas data frame
        reactions = table.df()
counts = reactions.sum(0).sort_values(0, ascending=False)
```

The `df` method above returns all the data in the cursor as a data frame. You can also fetch data in
chunks by passing the `chunk_size` argument to the `df` method.

Once your data is in a Pandas dataframe, you can transform it as needed.

## Other transforming tools

If you want to transform the data before loading, you can use Python. If you want to transform the
data after loading, you can use Pandas or one of the following:

1. [dbt.](dbt/dbt.md) (recommended)
2. [`dlt` SQL client.](sql.md)

