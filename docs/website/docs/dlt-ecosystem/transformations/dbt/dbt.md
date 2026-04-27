---
title: Transform data with dbt
description: Transforming the data loaded by a dlt pipeline with dbt
keywords: [transform, dbt, runner, dbt cloud]
---

# Transform data with dbt

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


## dbt Cloud

### API client

The dbt Cloud Client is a Python class designed to interact with the dbt Cloud API (version 2).
It provides methods to perform various operations on dbt Cloud, such as triggering job runs and retrieving job run statuses.

```py
from dlt.helpers.dbt_cloud import DBTCloudClientV2

# Initialize the client
client = DBTCloudClientV2(api_token="YOUR_API_TOKEN", account_id="YOUR_ACCOUNT_ID")

# Example: Trigger a job run
job_run_id = client.trigger_job_run(job_id=1234, data={"cause": "Triggered via API"})
print(f"Job run triggered successfully. Run ID: {job_run_id}")

# Example: Get run status
run_status = client.get_run_status(run_id=job_run_id)
print(f"Job run status: {run_status['status_humanized']}")
```

### Helper functions

These Python functions provide an interface to interact with the dbt Cloud API.
They simplify the process of triggering and monitoring job runs in dbt Cloud.

#### `run_dbt_cloud_job()`

This function triggers a job run in dbt Cloud using the specified configuration.
It supports various customization options and allows for monitoring the job's status.

```py
from dlt.helpers.dbt_cloud import run_dbt_cloud_job

# Trigger a job run with default configuration
status = run_dbt_cloud_job()

# Trigger a job run with additional data
additional_data = {
    "git_sha": "abcd1234",
    "schema_override": "custom_schema",
    # ... other parameters
}
status = run_dbt_cloud_job(job_id=1234, data=additional_data, wait_for_outcome=True)
```

#### `get_dbt_cloud_run_status()`

If you have already started a job run and have a run ID, then you can use the `get_dbt_cloud_run_status` function.

This function retrieves the full information about a specific dbt Cloud job run.
It also supports options for waiting until the run is complete.

```py
from dlt.helpers.dbt_cloud import get_dbt_cloud_run_status

# Retrieve status for a specific run
status = get_dbt_cloud_run_status(run_id=1234, wait_for_outcome=True)
```

### Set credentials

#### secrets.toml

When using dlt locally, we recommend using the `.dlt/secrets.toml` method to set credentials.

If you used the `dlt init` command, then the `.dlt` folder has already been created.
Otherwise, create a `.dlt` folder in your working directory and a `secrets.toml` file inside it.

This is where you store sensitive information securely, like access tokens. Keep this file safe.

Use the following format for dbt Cloud API authentication:

```toml
[dbt_cloud]
api_token = "set me up!" # required for authentication
account_id = "set me up!" # required for both helper functions
job_id = "set me up!" # optional only for the run_dbt_cloud_job function (you can pass this explicitly as an argument to the function)
run_id = "set me up!" # optional for the get_dbt_cloud_run_status function (you can pass this explicitly as an argument to the function)
```

#### Environment variables

dlt supports reading credentials from the environment.

If dlt tries to read this from environment variables, it will use a different naming convention.

For environment variables, all names are capitalized and sections are separated with a double underscore "__".

For example, for the above secrets, we would need to put into the environment:

```sh
DBT_CLOUD__API_TOKEN
DBT_CLOUD__ACCOUNT_ID
DBT_CLOUD__JOB_ID
```

For more information, read the [Credentials](../../../general-usage/credentials) documentation.
