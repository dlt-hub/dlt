---
sidebar_position: 3
---

# Transforming the data

If you want to transform the data before loading, you can use Python.

If you want to transform the data after loading, we recommend using [`dbt`](https://github.com/dbt-labs/dbt-core). Using the native SQL client from `dlt` is also a good approach for simple transformations after loading.

**What does `dbt` do?**

`dbt` is a framework that allows simple structuring of your transformations into DAGs.

The benefits of using `dbt` include
- end to end cross-db compatibility for dlt→dbt pipelines
- easy to use by sql analysts, low learning curve
- highly flexible and configurable in usage, supports templating, can run backfills etc.
- supports testing and accelerates troubleshooting.

**What does the `dbt` runner in `dlt` do?**

The `dbt` runner that comes with `dlt`
- can create a virtual env for dbt on the fly
- can run a dbt package from online (e.g. GitHub) or from local files
- passes configuration and credentials to dbt, so you do not need to handle them separately from dlt, enabling dbt to configured on the fly

**How can I use the `dbt` runner?**

Here’s an example for running a package from GitHub: [jaffle shop example](https://github.com/dlt-hub/dlt/blob/devel/docs/examples/dbt_run_jaffle.py)

Here is an example where we run a `dlt` pipeline and then a `dbt` package via `dlt`:

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