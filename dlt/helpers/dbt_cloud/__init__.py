import time
from typing import Any, Dict, Optional, Union

import dlt
from dlt.common.configuration import known_sections, with_config
from dlt.helpers.dbt_cloud.client import DBTCloudClientV2
from dlt.helpers.dbt_cloud.configuration import DBTCloudConfiguration


@with_config(
    spec=DBTCloudConfiguration,
    sections=(known_sections.DBT_CLOUD,),
)
def run_dbt_cloud_job(
    credentials: DBTCloudConfiguration = dlt.secrets.value,
    job_id: Union[int, str, None] = None,
    data: Optional[Dict[Any, Any]] = None,
    wait_for_outcome: bool = True,
    wait_seconds: int = 10,
) -> Dict[Any, Any]:
    """
    Trigger a dbt Cloud job run and retrieve its status.

    Args:
        credentials (DBTCloudConfiguration): Configuration parameters for dbt Cloud.
            Defaults to dlt.secrets.value.
        job_id (int | str, optional): The ID of the specific job to run.
            If not provided, it will use the job ID specified in the credentials.
            Defaults to None.
        data (dict, optional): Additional data to include when triggering the job run.
            Defaults to None.
            Fields of data:
                 '{
                    "cause": "string",
                    "git_sha": "string",
                    "git_branch": "string",
                    "azure_pull_request_id": integer,
                    "github_pull_request_id": integer,
                    "gitlab_merge_request_id": integer,
                    "schema_override": "string",
                    "dbt_version_override": "string",
                    "threads_override": integer,
                    "target_name_override": "string",
                    "generate_docs_override": boolean,
                    "timeout_seconds_override": integer,
                    "steps_override": [
                        "string"
                    ]
                }'
        wait_for_outcome (bool, optional): Whether to wait for the job run to complete before returning.
            Defaults to True.
        wait_seconds (int, optional): The interval (in seconds) between status checks while waiting for completion.
            Defaults to 10.

    Returns:
        dict: A dictionary containing the status information of the job run.

    Raises:
        InvalidCredentialsException: If account_id or job_id is missing.
    """
    operator = DBTCloudClientV2(
        api_token=credentials.api_token,
        account_id=credentials.account_id,
    )
    json_data = {"cause": credentials.cause}

    if credentials.git_sha:
        json_data["git_sha"] = credentials.git_sha

    elif credentials.git_branch:
        json_data["git_branch"] = credentials.git_branch

    if credentials.schema_override:
        json_data["schema_override"] = credentials.schema_override

    if data:
        json_data.update(json_data)

    job_id = job_id or credentials.job_id

    run_id = operator.trigger_job_run(job_id=job_id, data=json_data)
    status = operator.get_run_status(run_id)

    if wait_for_outcome and status["in_progress"]:
        while True:
            status = operator.get_run_status(run_id)
            if not status["in_progress"]:
                break

            time.sleep(wait_seconds)

    return status


@with_config(
    spec=DBTCloudConfiguration,
    sections=(known_sections.DBT_CLOUD,),
)
def get_dbt_cloud_run_status(
    credentials: DBTCloudConfiguration = dlt.secrets.value,
    run_id: Union[int, str, None] = None,
    wait_for_outcome: bool = True,
    wait_seconds: int = 10,
) -> Dict[Any, Any]:
    """
    Retrieve the status of a dbt Cloud job run.

    Args:
        credentials (DBTCloudConfiguration): Configuration parameters for dbt Cloud.
            Defaults to dlt.secrets.value.
        run_id (int | str, optional): The ID of the specific job run to retrieve status for.
            If not provided, it will use the run ID specified in the credentials.
            Defaults to None.
        wait_for_outcome (bool, optional): Whether to wait for the job run to complete before returning.
            Defaults to True.
        wait_seconds (int, optional): The interval (in seconds) between status checks while waiting for completion.
            Defaults to 10.

    Returns:
        dict: A dictionary containing the status information of the specified job run.

    Raises:
        InvalidCredentialsException: If account_id or run_id is missing.
    """
    operator = DBTCloudClientV2(
        api_token=credentials.api_token,
        account_id=credentials.account_id,
    )
    run_id = run_id or credentials.run_id
    status = operator.get_run_status(run_id)

    if wait_for_outcome and status["in_progress"]:
        while True:
            status = operator.get_run_status(run_id)
            if not status["in_progress"]:
                break

            time.sleep(wait_seconds)

    return status
