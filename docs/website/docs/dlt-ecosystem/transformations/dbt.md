---
title: Transforming data with dbt
description: Transforming the data loaded by a dlt pipeline with dbt
keywords: [transform, dbt, runner]
---

# Transforming data using dbt

[dbt](https://github.com/dbt-labs/dbt-core) is a framework that allows simple structuring of your transformations into DAGs. The benefits of
using dbt include:

- End-to-end cross-db compatibility for dlt→dbt pipelines.
- Easy to use by sql analysts, low learning curve.
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

## Other transforming tools

If you want to transform the data before loading, you can use Python. If you want to transform the
data after loading, you can use dbt or one of the following:

1. [`dlt` SQL client.](sql.md)
1. [Pandas.](pandas.md)
