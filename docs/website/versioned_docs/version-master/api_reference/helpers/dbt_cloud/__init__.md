---
sidebar_label: dbt_cloud
title: helpers.dbt_cloud
---

## run\_dbt\_cloud\_job

```python
@with_config(
    spec=DBTCloudConfiguration,
    sections=(known_sections.DBT_CLOUD, ),
)
def run_dbt_cloud_job(credentials: DBTCloudConfiguration = dlt.secrets.value,
                      job_id: Union[int, str, None] = None,
                      data: Optional[Dict[Any, Any]] = None,
                      wait_for_outcome: bool = True,
                      wait_seconds: int = 10) -> Dict[Any, Any]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/helpers/dbt_cloud/__init__.py#L14)

Trigger a dbt Cloud job run and retrieve its status.

**Arguments**:

- `credentials` _DBTCloudConfiguration_ - Configuration parameters for dbt Cloud.
  Defaults to dlt.secrets.value.
- `job_id` _int | str, optional_ - The ID of the specific job to run.
  If not provided, it will use the job ID specified in the credentials.
  Defaults to None.
- `data` _dict, optional_ - Additional data to include when triggering the job run.
  Defaults to None.
  Fields of data:
  '{
- `"cause"` - "string",
- `"git_sha"` - "string",
- `"git_branch"` - "string",
- `"azure_pull_request_id"` - integer,
- `"github_pull_request_id"` - integer,
- `"gitlab_merge_request_id"` - integer,
- `"schema_override"` - "string",
- `"dbt_version_override"` - "string",
- `"threads_override"` - integer,
- `"target_name_override"` - "string",
- `"generate_docs_override"` - boolean,
- `"timeout_seconds_override"` - integer,
- `"steps_override"` - [
  "string"
  ]
  }'
- `wait_for_outcome` _bool, optional_ - Whether to wait for the job run to complete before returning.
  Defaults to True.
- `wait_seconds` _int, optional_ - The interval (in seconds) between status checks while waiting for completion.
  Defaults to 10.
  

**Returns**:

- `dict` - A dictionary containing the status information of the job run.
  

**Raises**:

- `InvalidCredentialsException` - If account_id or job_id is missing.

## get\_dbt\_cloud\_run\_status

```python
@with_config(
    spec=DBTCloudConfiguration,
    sections=(known_sections.DBT_CLOUD, ),
)
def get_dbt_cloud_run_status(
        credentials: DBTCloudConfiguration = dlt.secrets.value,
        run_id: Union[int, str, None] = None,
        wait_for_outcome: bool = True,
        wait_seconds: int = 10) -> Dict[Any, Any]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/helpers/dbt_cloud/__init__.py#L99)

Retrieve the status of a dbt Cloud job run.

**Arguments**:

- `credentials` _DBTCloudConfiguration_ - Configuration parameters for dbt Cloud.
  Defaults to dlt.secrets.value.
- `run_id` _int | str, optional_ - The ID of the specific job run to retrieve status for.
  If not provided, it will use the run ID specified in the credentials.
  Defaults to None.
- `wait_for_outcome` _bool, optional_ - Whether to wait for the job run to complete before returning.
  Defaults to True.
- `wait_seconds` _int, optional_ - The interval (in seconds) between status checks while waiting for completion.
  Defaults to 10.
  

**Returns**:

- `dict` - A dictionary containing the status information of the specified job run.
  

**Raises**:

- `InvalidCredentialsException` - If account_id or run_id is missing.

