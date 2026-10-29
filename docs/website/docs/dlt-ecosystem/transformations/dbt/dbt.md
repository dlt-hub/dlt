---
title: Transform data with dbt
description: Transforming the data loaded by a dlt pipeline with dbt
keywords: [transform, dbt, runner, dbt cloud]
---
# Transform data with dbt

[dbt](https://github.com/dbt-labs/dbt-core) is a framework that structures your transformations as DAGs. dbt gives you these benefits:

- End-to-end cross-db compatibility for dlt→dbt pipelines.
- Ease of use by SQL analysts, with a low learning curve.
- Flexible configuration, templating and backfills.
- Support for testing and faster troubleshooting.

## dbt runner in dlt

The dbt runner runs dbt from `dlt`.

The dbt runner:

- Can create a virtual environment for dbt when it runs.
- Can run a dbt package from a remote repository (for example, GitHub) or from local files.
- Passes the config and credentials to dbt. You do not need to configure dbt separately from `dlt`.
- Passes the dataset name, the load id and the time interval of the load to the dbt package.

The dbt runner requires dbt Core 1.9.3 or later (and below 2.0).

## How to use the dbt runner

For an example of how to use the dbt runner, see the [jaffle shop example](https://github.com/dlt-hub/dlt/blob/devel/docs/examples/archive/dbt_run_jaffle.py).
The example below runs a `dlt` pipeline and then runs a dbt package with `dlt`:

:::info
Your IDE shows the docstrings of the runner.
:::

```py
# Load all Pipedrive endpoints to the pipedrive_raw dataset
pipeline = dlt.pipeline(
    pipeline_name='pipedrive',
    destination='bigquery',
    dataset_name='pipedrive_raw'
)

load_info = pipeline.run(pipedrive_source())
print(load_info)

# The dbt models go to a new dataset called 'pipedrive_dbt'
# The local dbt package has pipedrive_raw in its sources.yml
# The pipeline gives the destination of the models
pipeline = dlt.pipeline(
    pipeline_name='pipedrive',
    destination='bigquery',
    dataset_name='pipedrive_dbt'
)

# Create or restore a venv with the newest dbt version that dlt supports
# NOTE: If dbt is installed in your current environment, do not create the venv
#       and do not pass the `venv` argument to dlt.dbt.package()
venv = dlt.dbt.get_venv(pipeline)

# Create the runner. The venv argument is optional
dbt = dlt.dbt.package(
    pipeline,
    "pipedrive/dbt_pipedrive/pipedrive",
    venv=venv
)

# Run the models and collect the results
# If the run fails, the runner raises an error with the full stack trace
models = dbt.run_all()

# If the run succeeds, print the results
for m in models:
    print(
        f"Model {m.model_name} materialized" +
        f" in {m.time}" +
        f" with status {m.status}" +
        f" and message {m.message}" +
        f" ({m.rows_affected} rows)"
    )
```

`dlt.dbt.get_venv` creates a virtual environment in the pipeline working directory and installs `dbt-core` and the dbt adapter for the pipeline destination. Pass `dbt_version` to pin the version, for example `dlt.dbt.get_venv(pipeline, dbt_version="1.10.4")`.

`run_all` runs the typical dbt workflow:

1. Clones the package, or updates it from the remote repository. The runner does not change a package at a local path.
2. Installs the dependencies (`dbt deps`).
3. Runs the source tests that `source_tests_selector` selects. If the tests fail, the runner raises `PrerequisitesException` and does not run the models.
4. Loads the seeds (`dbt seed`).
5. Runs the package (`dbt run`) with `run_params` (default: `--fail-fast`).
6. If an incremental model fails because its schema is [out of sync](https://docs.getdbt.com/docs/build/incremental-models#what-if-the-columns-of-my-incremental-model-change), it repeats the run with `--full-refresh`. To disable the retry, set `auto_full_refresh_when_out_of_sync=False`.

You can also run the dbt commands separately with `dbt.run()` and `dbt.test()`.

### Run and test results

`run_all`, `run` and `test` return one `DBTNodeResult` for each model, seed or test. Each result has these fields:

- `model_name`, `status`, `message` and `time`: the name, status, message and run time that dbt reports.
- `load_id`: the [load id](#load-id-of-the-data-produced-by-dbt) that you passed to the runner, or `None`.
- `rows_affected`: the number of rows that the last statement of a model changed, as the dbt adapter reports it.
- `failures`: the number of failing rows of a data test.
- `adapter_response`: the full response of the dbt adapter, for example `bytes_processed` on BigQuery.

The value of `rows_affected` depends on the dbt adapter and on the materialization:

- Seeds always report the number of loaded rows.
- A `merge` statement reports one total for inserted and updated rows.
- A `delete+insert` strategy reports only the inserted rows.
- Views, duckdb models and microbatch models report no count, so `rows_affected` is `None`.

The runner disables dbt [anonymous usage stats](https://docs.getdbt.com/reference/global-configs/usage-stats). To enable them, set the `send_anonymous_usage_stats` option of the runner:

```toml
[dbt_package_runner]
send_anonymous_usage_stats = true
```

## Variables passed to the dbt package

The runner passes the following [dbt variables](https://docs.getdbt.com/docs/build/project-variables) to every dbt command:

- `source_dataset_name`: the dataset name of the pipeline. Use it in your `sources.yml` as the source schema.
- `destination_dataset_name`: the dataset in which dbt creates the models, if you pass `destination_dataset_name` to `run_all`. The dbt profiles of `dlt` use it as the target schema. If it is not set, they use `source_dataset_name`.
- `load_id`: the load id of the data produced by the package, only if you pass `load_id` to `run_all`, `run` or `test`. See [below](#load-id-of-the-data-produced-by-dbt).
- any variables passed in `additional_vars` or in the `package_additional_vars` option. They override the variables above.

### Load id of the data produced by dbt

Tables loaded by `dlt` have a `_dlt_load_id` column that identifies the load package of each row. To make the tables created by dbt compatible with `dlt`, generate a load id, pass it to the runner and write the `load_id` variable into this column:

```py
from dlt.common.storages import create_load_id

dbt = dlt.dbt.package(pipeline, "pipedrive/dbt_pipedrive/pipedrive")
load_id = create_load_id()
models = dbt.run_all(load_id=load_id)
```

```sql
select deal_id, value, '{{ var("load_id") }}' as _dlt_load_id
from {{ source('pipedrive', 'deals') }}
```

`load_id` identifies the data that dbt creates, not the data it processes. Do not pass the load id of the package that `dlt` loaded. All the commands in one `run_all` call use the same load id. Every result returned by the runner carries it, including the results attached to `DBTProcessingError`, so you can find the rows written before a failed run.

:::note
dbt re-parses the whole project when its variables change. A new load id in every run disables dbt [partial parsing](https://docs.getdbt.com/reference/parsing#partial-parsing), which may slow down large projects. Pass `load_id` only if your package uses it.
:::

If you do not want to repeat the expression in every model, add a macro to the `macros` folder of your package. When you do not pass `load_id`, or the package runs without `dlt` (for example, with `dbt run`), the macro uses the start time of the dbt invocation. This value has the same format as a `dlt` load id and does not change dbt variables. All models in one dbt invocation get the same value:

```sql
-- macros/dlt_load_id.sql
{% macro dlt_load_id() -%}
'{{ var("load_id", run_started_at.timestamp() | string) }}'
{%- endmacro %}
```

Use the macro in your models:

```sql
select deal_id, value, {{ dlt_load_id() }} as _dlt_load_id
from {{ source('pipedrive', 'deals') }}
```

## Microbatch models and time intervals

dbt [microbatch models](https://docs.getdbt.com/docs/build/incremental-microbatch) split the processed data into time batches. dbt filters the model's inputs to each batch, so the model SQL has no incremental filter. Set the time column of the model with `event_time`. Set the time column of each input with `event_time` in `sources.yml`, for example for a table that `dlt` loaded:

```yaml
sources:
  - name: pipedrive
    schema: "{{ var('source_dataset_name') }}"
    tables:
      - name: deals
        config:
          event_time: update_time
```

```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='microbatch',
        event_time='update_time',
        begin='2024-01-01',
        batch_size='day',
        unique_key='deal_id',
    )
}}

select deal_id, value, update_time, {{ dlt_load_id() }} as _dlt_load_id
from {{ source('pipedrive', 'deals') }}
```

By default, dbt processes the batches from `begin` (on the first run) or from the last few batches up to the current time. Pass `interval` to `run_all` or `run` to process exactly the time range that `dlt` loaded. An interval is useful when an [external scheduler](../../../general-usage/incremental/cursor.md#using-airflow-schedule-for-backfill-and-incremental-loading) like Airflow runs your pipeline, or when you backfill data:

```py
from datetime import datetime, timezone

from dlt.common.configuration.container import Container
from dlt.extract.incremental.context import TimeIntervalContext

dbt = dlt.dbt.package(pipeline, "pipedrive/dbt_pipedrive/pipedrive")

interval = (datetime(2024, 3, 1, tzinfo=timezone.utc), datetime(2024, 3, 8, tzinfo=timezone.utc))
with Container().injectable_context(TimeIntervalContext(interval=interval)):
    # incremental resources with allow_external_schedulers=True load the interval
    pipeline.run(pipedrive_source())
    # microbatch models process the same interval
    models = dbt.run_all(interval=dlt.current.interval())
```

The interval is a `(start, end)` tuple of datetimes, where `start` is inclusive and `end` is exclusive. dbt also extends the interval to whole batches.

The runner passes the interval to `dbt run` as `--event-time-start` and `--event-time-end`. The runner converts both bounds to UTC. It reads naive datetimes in the [context timezone](../../../general-usage/schema.md#context-timezone). dbt accepts whole seconds only. If a bound has fractional seconds, the runner widens the interval to whole seconds. See [injecting and reading the current interval](../../../general-usage/incremental/cursor.md#injecting-and-reading-the-current-interval) for other ways to set the interval.

Models that are not microbatch models ignore the interval.

## How to run the dbt runner without a pipeline

You can use the dbt runner without a dlt pipeline. The example below clones and runs **jaffle shop** with a dbt profile that you supply.
The example assumes that dbt is installed in the current Python environment. It also assumes that `profiles.yml` is in the same folder as the Python script.

```py notype
import os

from dlt.helpers.dbt import create_runner

runner = create_runner(
    None,  # run dbt in the current virtual environment
    None,  # no dataset name and no credentials
    working_dir=".",  # the runner clones the package into the current folder
    package_location="https://github.com/dbt-labs/jaffle_shop.git",
    package_profiles_dir=os.path.abspath(
        "."
    ),  # profiles.yml must be in this folder
    package_profile_name="duckdb_dlt_dbt_test",  # name of the profile
)

models = runner.run_all()
```

This is an example **duckdb** profile:

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

To see the dbt debug log, run the example with this command: `RUNTIME__LOG_LEVEL=DEBUG python dbt_standalone.py`

## dbt Cloud

### API client

The dbt Cloud Client is a Python class for the dbt Cloud API (version 2).
Its methods start job runs and get the status of job runs.

```py
from dlt.helpers.dbt_cloud import DBTCloudClientV2

# Create the client
client = DBTCloudClientV2(api_token="YOUR_API_TOKEN", account_id="YOUR_ACCOUNT_ID")

# Example: start a job run
job_run_id = client.trigger_job_run(job_id=1234, data={"cause": "Triggered via API"})
print(f"Started job run with run ID {job_run_id}")

# Example: get the status of the job run
run_status = client.get_run_status(run_id=job_run_id)
print(f"Job run status: {run_status['status_humanized']}")
```

### Helper functions

These Python functions use the dbt Cloud API.
They start job runs in dbt Cloud and monitor them.

#### `run_dbt_cloud_job()`

This function starts a job run in dbt Cloud with the specified config.
It can also wait until the job run is complete and return its status.

```py
from dlt.helpers.dbt_cloud import run_dbt_cloud_job

# Start a job run with the default config
status = run_dbt_cloud_job()

# Start a job run with additional data
additional_data = {
    "git_sha": "abcd1234",
    "schema_override": "custom_schema",
    # ... other parameters
}
status = run_dbt_cloud_job(job_id=1234, data=additional_data, wait_for_outcome=True)
```

#### `get_dbt_cloud_run_status()`

If you started a job run and have its run ID, use the `get_dbt_cloud_run_status` function.

This function gets all the information about one dbt Cloud job run.
It can also wait until the job run is complete.

```py
from dlt.helpers.dbt_cloud import get_dbt_cloud_run_status

# Get the status of one job run
status = get_dbt_cloud_run_status(run_id=1234, wait_for_outcome=True)
```

### Set credentials

#### secrets.toml

If you run dlt locally, set the credentials in `.dlt/secrets.toml`.

If you used the `dlt init` command, the `.dlt` folder already exists.
Otherwise, create a `.dlt` folder in your working directory and a `secrets.toml` file inside it.

This file holds sensitive information, for example access tokens. Keep this file safe.

Use the following format for dbt Cloud API authentication:

```toml
[dbt_cloud]
api_token = "set me up!" # required for authentication
account_id = "set me up!" # required for both helper functions
job_id = "set me up!" # optional, used by run_dbt_cloud_job (you can also pass it as an argument)
run_id = "set me up!" # optional, used by get_dbt_cloud_run_status (you can also pass it as an argument)
```

#### Environment variables

dlt can read credentials from environment variables. These variables use a different naming convention.

The names are in upper case, and a double underscore (`__`) separates the sections.

For example, set these environment variables for the secrets in the previous example:

```sh
DBT_CLOUD__API_TOKEN
DBT_CLOUD__ACCOUNT_ID
DBT_CLOUD__JOB_ID
```

For more information, read the [Credentials](../../../general-usage/credentials/index.md) documentation.
