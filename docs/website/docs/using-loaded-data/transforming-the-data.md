---
title: Transforming the data
description: Transforming the data loaded by a dlt pipeline
keywords: [transform, dbt, runner]
---

# Transforming the data

If you want to transform the data before loading, you can use Python. If you want to transform the data after loading, you can use one of the following:
1. [`dbt`](https://github.com/dbt-labs/dbt-core) (recommended)
2. `dlt` SQL client
3. Pandas

## Transforming the data using dbt

`dbt` is a framework that allows simple structuring of your transformations into DAGs. The benefits of using `dbt` include
- end to end cross-db compatibility for dlt→dbt pipelines.
- easy to use by sql analysts, low learning curve.
- highly flexible and configurable in usage, supports templating, can run backfills etc.
- supports testing and accelerates troubleshooting.

**`dbt` runner in `dlt`**

You can run `dbt` with `dlt` by using the `dbt` runner. The `dbt` runner
- can create a virtual env for dbt on the fly.
- can run a dbt package from online (e.g. GitHub) or from local files.
- passes configuration and credentials to dbt, so you do not need to handle them separately from dlt, enabling dbt to configured on the fly.

**How to use the `dbt` runner?**

For an example of how to use the `dbt` runner, see the [jaffle shop example](https://github.com/dlt-hub/dlt/blob/devel/docs/examples/dbt_run_jaffle.py).
Included below in another example where we run a `dlt` pipeline and then a `dbt` package via `dlt`:

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

A simple alternative to `dbt` is to query the data using the `dlt` native sql client and then performing the transformations using Python.

```python
pipeline = dlt.pipeline(destination="bigquery", dataset_name="tweets")
try:
    with pipeline.sql_client() as client:
        res = client.execute_sql(last_value_query)
        last_value = res[0][0]
```

## Transforming the data using Pandas

It is also possible to carry out data transformations directly in Pandas. For this, you will need to define a function that can convert the results of a sql query into a Pandas DataFrame. 

As an example for how to do this:
1. Construct a function `query_results_to_df` and pass as input parameters a sql client (e.g., the dlt sql client) and the query that you want to execute.
2. Convert the results of the query into a dataframe by including the rows returned from `curr.fetchall()` inside the `_wrap_result` method in Pandas.

```python
import pandas as pd
from pandas.io.sql import _wrap_result

def query_results_to_df(client, query, index_col = None, coerce_float = True, parse_dates = None, dtype = None):
    # dlt sql client returns DB API compatible cursor
    with client.execute_query(query) as curr:
        # get column names
        columns = [c[0] for c in curr.description]
        # use existing panda function that converts results to data frame
        # TODO: we may use `_wrap_iterator` to prevent loading the full result to memory first
        df = _wrap_result(curr.fetchall(), columns, index_col, coerce_float, parse_dates, dtype)
        return df
```
