---
title: Transforming the data
description: Transforming the data loaded by a dlt pipeline
keywords: [transform, dbt, runner]
---

# Transforming the data

If you want to transform the data before loading, you can use Python. If you want to transform the data after loading, you can use one of the following:
1. [dbt](https://github.com/dbt-labs/dbt-core) (recommended)
2. `dlt` SQL client
3. Pandas

## Transforming the data using dbt

dbt is a framework that allows simple structuring of your transformations into DAGs. The benefits of using dbt include
- end to end cross-db compatibility for dlt→dbt pipelines.
- easy to use by sql analysts, low learning curve.
- highly flexible and configurable in usage, supports templating, can run backfills etc.
- supports testing and accelerates troubleshooting.

**dbt runner in `dlt`**

You can run dbt with `dlt` by using the dbt runner. The dbt runner
- can create a virtual env for dbt on the fly.
- can run a dbt package from online (e.g. GitHub) or from local files.
- passes configuration and credentials to dbt, so you do not need to handle them separately from dlt, enabling dbt to configured on the fly.

**How to use the dbt runner**

For an example of how to use the dbt runner, see the [jaffle shop example](https://github.com/dlt-hub/dlt/blob/devel/docs/examples/dbt_run_jaffle.py).
Included below in another example where we run a `dlt` pipeline and then a dbt package via `dlt`:

> **💡**  Docstrings are available to read in your IDE

```python

# load all pipedrive endpoints to pipedrive_raw dataset
pipeline = dlt.pipeline(pipeline_name='pipedrive',
						destination='bigquery',
						dataset_name='pipedrive_raw')

load_info = pipeline.run(pipedrive_source())
print(load_info)

# Create a transformation on a new dataset called 'pipedrive_dbt'
# we created a local dbt package
# and added pipedrive_raw to its sources.yml
# the destination for the transformation is passed in the pipeline
pipeline = dlt.pipeline(pipeline_name='pipedrive',
						destination='bigquery',
						dataset_name='pipedrive_dbt')

# make or restore venv for dbt, using latest dbt version
venv = dlt.dbt.get_venv(pipeline)

# get runner, optionally pass the venv
dbt = dlt.dbt.package(pipeline,
							        "pipedrive/dbt_pipedrive/pipedrive", #
							        venv=venv)

# run the models and collect any info
# If running fails, the error will be raised with full stack trace
models = dbt.run_all()

# on success print outcome
for m in models:
        print(f"Model {m.model_name} materialized in {m.time} with status {m.status} and message {m.message}")
```

## Transforming the data using the `dlt` SQL client

A simple alternative to dbt is to query the data using the `dlt` sql client and then performing the transformations using Python. The `execute_sql` method allows you to execute any SQL statement, including statements that change the database schema or data in the tables. In the example below we insert a row into `customers` table. Note that the syntax is the same as for any standard `dbapi` connection.

```python
pipeline = dlt.pipeline(destination="bigquery", dataset_name="crm")
try:
    with pipeline.sql_client() as client:
        client.sql_client.execute_sql(f"INSERT INTO customers VALUES (%s, %s, %s)", 10, "Fred", "fred@fred.com")
```

In the case of SELECT queries, the data is returned as a list of row, with the elements of a row corresponding to selected columns.

```python
try:
    with pipeline.sql_client() as client:
        res = client.execute_sql("SELECT id, name, email FROM customers WHERE id = %s", 10)
        # prints columns values of first row
        print(res[0])
```

## Transforming the data using Pandas

You can fetch results of any SQL query as a data frame. If the destination is supporting that natively (ie. BigQuery and DuckDB), `dlt` uses the native method. Thanks to that, reading data frames may be really fast! The example below reads GitHub reactions data from the `issues` table and counts reaction types.

```python
pipeline = dlt.pipeline(pipeline_name="github_pipeline", destination="duckdb", dataset_name="github_reactions", full_refresh=True)
with pipeline.sql_client() as client:
    with client.execute_query('SELECT "reactions__+1", "reactions__-1", reactions__laugh, reactions__hooray, reactions__rocket FROM issues') as table:
        # calling `df` on a cursor, returns the data as a data frame
        reactions = table.df()
counts = reactions.sum(0).sort_values(0, ascending=False)
```

The `df` method above returns all the data in the cursor as data frame. You can also fetch data in chunks by passing `chunk_size` argument to the `df` method.

Once your data is in a Pandas data frame, you can transform it as needed.