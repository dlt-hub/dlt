---
title: Transform the data with dbt
description: Transforming the data loaded by a dlt pipeline with dbt
keywords: [transform, dbt, runner]
---

# Transform the data with dbt

[dbt](https://github.com/dbt-labs/dbt-core) is a framework that allows for the simple structuring of your transformations into DAGs. The benefits of using dbt include:

- End-to-end cross-db compatibility for dlt→dbt pipelines.
- Ease of use by SQL analysts, with a low learning curve.
- High flexibility and configurability in usage, supports templating, can run backfills, etc.
- Support for testing and accelerated troubleshooting.

## dbt runner in dlt

You can run dbt with `dlt` by using the dbt runner.

The dbt runner:

- Can create a virtual environment for dbt on the fly;
- Can run a dbt package from online sources (e.g., GitHub) or from local files;
- Passes configuration and credentials to dbt, so you do not need to handle them separately from `dlt`, enabling dbt to configure on the fly.

## How to use the dbt runner

For an example of how to use the dbt runner, see the [jaffle shop example](https://github.com/dlt-hub/dlt/blob/devel/docs/examples/archive/dbt_run_jaffle.py).
Included below is another example where we run a `dlt` pipeline and then a dbt package via `dlt`:

> 💡 Docstrings are available to read in your IDE.

```py
# Load all Pipedrive endpoints to the pipedrive_raw dataset
pipeline = dlt.pipeline(
    pipeline_name='pipedrive',
    destination='bigquery',
    dataset_name='pipedrive_raw'
)

load_info = pipeline.run(pipedrive_source())
print(load_info)

# Create a transformation on a new dataset called 'pipedrive_dbt'
# We created a local dbt package
# and added pipedrive_raw to its sources.yml
# The destination for the transformation is passed in the pipeline
pipeline = dlt.pipeline(
    pipeline_name='pipedrive',
    destination='bigquery',
    dataset_name='pipedrive_dbt'
)

# Make or restore venv for dbt, using the latest dbt version
# NOTE: If you have dbt installed in your current environment, just skip this line
#       and the `venv` argument to dlt.dbt.package()
venv = dlt.dbt.get_venv(pipeline)

# Get runner, optionally pass the venv
dbt = dlt.dbt.package(
    pipeline,
    "pipedrive/dbt_pipedrive/pipedrive",
    venv=venv
)

# Run the models and collect any info
# If running fails, the error will be raised with a full stack trace
models = dbt.run_all()

# On success, print the outcome
for m in models:
    print(
        f"Model {m.model_name} materialized" +
        f" in {m.time}" +
        f" with status {m.status}" +
        f" and message {m.message}"
    )
```

## How to run dbt runner without pipeline
You can use the dbt runner without a dlt pipeline. The example below will clone and run **jaffle shop** using a dbt profile that you supply.
It assumes that dbt is installed in the current Python environment and the `profile.yml` is in the same folder as the Python script.
<!--@@@DLT_SNIPPET ./dbt-snippets.py::run_dbt_standalone-->


Here's an example **duckdb** profile:
```yaml
config:
  # Do not track usage, do not create .user.yml
  send_anonymous_usage_stats: False

duckdb_dlt_dbt_test:
  target: analytics
  outputs:
    analytics:
      type: duckdb
      # Schema: "{{ var('destination_dataset_name', var('source_dataset_name')) }}"
      path: "duckdb_dlt_dbt_test.duckdb"
      extensions:
        - httpfs
        - parquet
```
You can run the example with dbt debug log: `RUNTIME__LOG_LEVEL=DEBUG python dbt_standalone.py`


## Other transforming tools

If you want to transform the data before loading, you can use Python. If you want to transform the data after loading, you can use dbt or one of the following:

1. [`dlt` SQL client.](../sql.md)
2. [Pandas.](../pandas.md)

