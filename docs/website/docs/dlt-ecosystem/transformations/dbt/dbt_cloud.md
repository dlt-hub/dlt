# DBT Cloud Helpers

These Python functions provide an interface to interact with the dbt Cloud API. They simplify the process of triggering and monitoring job runs in dbt Cloud.

## `run_dbt_cloud_job`

### Overview

This function triggers a job run in dbt Cloud using the specified configuration. It supports various customization options and allows for monitoring the job's status.

### Set credentials

If you used the `dlt init` command, then the `.dlt` folder has already been created.
Otherwise, create a `.dlt` folder in your working directory and a `secrets.toml` file inside it.

It's where you store sensitive information securely, like access tokens. Keep this file safe.

Use the following format for dbt Cloud API authentication:

```toml
[dbt_cloud]
api_token = "set me up!" # required for authentication
account_id = "set me up!" #
job_id = "set me up!"
```

For more information, read the [Credentials](https://dlthub.com/docs/general-usage/credentials) documentation.

### Usage

```python
from dlt.helpers.dbt_cloud import run_dbt_cloud_job

# Trigger a job run with default configuration
status = run_dbt_cloud_job()

# Trigger a job run with additional data
additional_data = {
    "git_sha": "abcd1234",
    "schema_override": "custom_schema",
    # ... other parameters
}
status = run_dbt_cloud_job(data=additional_data, wait_for_outcome=True)
```

### Parameters

- `credentials`: Configuration parameters for dbt Cloud.

- `data` (optional): Additional data to pass when triggering the job run, in the form of a dictionary.

- `wait_for_outcome` (optional): If set to `True`, the function will wait for the job run to complete before returning. Default is `True`.

- `wait_seconds` (optional): The interval (in seconds) at which to check the job's status while waiting for completion. Default is `10` seconds.

### Returns

A dictionary containing the status information of the job run.

## `get_dbt_cloud_run_status`

### Overview

This function retrieves the status of a specific dbt Cloud job run. It also supports options for waiting until the run is complete.

### Usage

```python
from dlt.helpers.dbt_cloud import get_dbt_cloud_run_status

# Retrieve status for a specific run
status = get_dbt_cloud_run_status(run_id=1234)
```

### Parameters

- `credentials`: Configuration parameters for dbt Cloud.

- `run_id` (optional): The ID of the specific job run to retrieve status for. If not provided, it will use the run ID specified in the credentials.

- `wait_for_outcome` (optional): If set to `True`, the function will wait for the job run to complete before returning. Default is `True`.

- `wait_seconds` (optional): The interval (in seconds) at which to check the job's status while waiting for completion. Default is `10` seconds.

### Returns

A dictionary containing the status information of the specified job run.
