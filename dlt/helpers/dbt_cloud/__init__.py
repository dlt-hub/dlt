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
    data: Optional[Dict[Any, Any]] = None,
    wait_for_outcome: bool = True,
    wait_seconds: int = 10,
) -> Dict[Any, Any]:
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

    run_id = operator.trigger_job_run(job_id=credentials.job_id, data=json_data)
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
