---
title: Transform the data with dbt
description: Transforming the data loaded by a dlt pipeline with dbt
keywords: [transform, dbt, runner]
---

# Transform the data with dbt

[dbt](https://github.com/dbt-labs/dbt-core) is a framework that allows simple structuring of your transformations into DAGs. The benefits of
using dbt include:

- End-to-end cross-db compatibility for dlt→dbt pipelines.
- Easy to use by SQL analysts, low learning curve.
- Highly flexible and configurable in usage, supports templating, can run backfills etc.
- Supports testing and accelerates troubleshooting.

## dbt runner in dlt

You can run dbt with `dlt` by using the dbt runner.

The dbt runner

- can create a virtual env for dbt on the fly;
- can run a dbt package from online (e.g. GitHub) or from local files;
- passes configuration and credentials to dbt, so you do not need to handle them separately from
  `dlt`, enabling dbt to configure on the fly.

## How to use the dbt runner

For an example of how to use the dbt runner, see the
[jaffle shop example](https://github.com/dlt-hub/dlt/blob/devel/docs/examples/archive/dbt_run_jaffle.py).
Included below in another example where we run a `dlt` pipeline and then a dbt package via `dlt`:

> 💡 Docstrings are available to read in your IDE.

```python
# load all pipedrive endpoints to pipedrive_raw dataset
pipeline = dlt.pipeline(
    pipeline_name='pipedrive',
    destination='bigquery',
    dataset_name='pipedrive_raw'
)

load_info = pipeline.run(pipedrive_source())
print(load_info)

# Create a transformation on a new dataset called 'pipedrive_dbt'
# we created a local dbt package
# and added pipedrive_raw to its sources.yml
# the destination for the transformation is passed in the pipeline
pipeline = dlt.pipeline(
    pipeline_name='pipedrive',
    destination='bigquery',
    dataset_name='pipedrive_dbt'
)

# make or restore venv for dbt, using latest dbt version
# NOTE: if you have dbt installed in your current environment, just skip this line
#       and the `venv` argument to dlt.dbt.package()
venv = dlt.dbt.get_venv(pipeline)

# get runner, optionally pass the venv
dbt = dlt.dbt.package(
    pipeline,
    "pipedrive/dbt_pipedrive/pipedrive",
    venv=venv
)

# run the models and collect any info
# If running fails, the error will be raised with full stack trace
models = dbt.run_all()

# on success print outcome
for m in models:
    print(
        f"Model {m.model_name} materialized" +
        f"in {m.time}" +
        f"with status {m.status}" +
        f"and message {m.message}"
    )
```

## How to run dbt runner without pipeline
You can use dbt runner without dlt pipeline. Example below will clone and run **jaffle shop** using a dbt profile that you supply.
It assumes that dbt is installed in the current Python environment and the `profile.yml` is in the same folder as the Python script.
<!--@@@DLT_SNIPPET_START ./dbt-snippets.py::run_dbt_standalone-->
```py
import os

from dlt.helpers.dbt import create_runner

runner = create_runner(
    None,  # use current virtual env to run dlt
    None,  # we do not need dataset name and we do not pass any credentials in environment to dlt
    working_dir=".",  # the package below will be cloned to current dir
    package_location="https://github.com/dbt-labs/jaffle_shop.git",
    package_profiles_dir=os.path.abspath("."),  # profiles.yml must be placed in this dir
    package_profile_name="duckdb_dlt_dbt_test",  # name of the profile
)

models = runner.run_all()
```
<!--@@@DLT_SNIPPET_END ./dbt-snippets.py::run_dbt_standalone-->

Here's example **duckdb** profile
```yaml
config:
  # do not track usage, do not create .user.yml
  send_anonymous_usage_stats: False

duckdb_dlt_dbt_test:
  target: analytics
  outputs:
    analytics:
      type: duckdb
      # schema: "{{ var('destination_dataset_name', var('source_dataset_name')) }}"
      path: "duckdb_dlt_dbt_test.duckdb"
      extensions:
        - httpfs
        - parquet
```
You can run the example with dbt debug log: `RUNTIME__LOG_LEVEL=DEBUG python dbt_standalone.py`


## Other transforming tools

If you want to transform the data before loading, you can use Python. If you want to transform the
data after loading, you can use dbt or one of the following:

1. [`dlt` SQL client.](../sql.md)
1. [Pandas.](../pandas.md)
