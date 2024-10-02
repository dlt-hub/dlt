---
title: Transforming the Data with dbt Cloud
description: Transforming the data loaded by a dlt pipeline with dbt Cloud
keywords: [transform, sql]
---

# dbt Cloud client and helper functions

## API client

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

## Helper functions

These Python functions provide an interface to interact with the dbt Cloud API.
They simplify the process of triggering and monitoring job runs in dbt Cloud.

### `run_dbt_cloud_job()`

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

### `get_dbt_cloud_run_status()`

If you have already started a job run and have a run ID, then you can use the `get_dbt_cloud_run_status` function.

This function retrieves the full information about a specific dbt Cloud job run.
It also supports options for waiting until the run is complete.

```py
from dlt.helpers.dbt_cloud import get_dbt_cloud_run_status

# Retrieve status for a specific run
status = get_dbt_cloud_run_status(run_id=1234, wait_for_outcome=True)
```

## Set credentials

### secrets.toml

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

### Environment variables

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

