import time
from typing import Any, Dict, Optional

import dlt
from dlt.common.configuration import with_config, known_sections
from dlt.helpers.dbt_cloud.client import DBTCloudClientV2
from dlt.helpers.dbt_cloud.configuration import DBTCloudConfiguration


@with_config(
    spec=DBTCloudConfiguration,
    sections=(known_sections.DBT_CLOUD,),
)
def run_dbt_cloud_job(
    data: Optional[dict],
    credentials: DBTCloudConfiguration = dlt.secrets.value,
    wait_for_outcome: bool = True,
    wait_seconds: int = 10,
) -> Dict[Any, Any]:
    operator = DBTCloudClientV2(
        api_token=credentials.api_token,
        account_id=credentials.account_id,
        job_id=credentials.job_id,
    )
    run_id = operator.trigger_job_run(data=data)

    status = {}
    if wait_for_outcome:
        while True:
            status = operator.get_job_status(run_id)
            if not status["in_progress"]:
                break

            time.sleep(wait_seconds)

    return status


@with_config(
    spec=DBTCloudConfiguration,
    sections=(known_sections.DBT_CLOUD,),
)
def get_dbt_cloud_run_status(
    run_id: int,
    credentials: DBTCloudConfiguration = dlt.secrets.value,
    wait_for_outcome: bool = True,
    wait_seconds: int = 10,
) -> Dict[Any, Any]:
    operator = DBTCloudClientV2(
        api_token=credentials.api_token,
        account_id=credentials.account_id,
        job_id=credentials.job_id,
    )
    status = operator.get_job_status(run_id)

    if wait_for_outcome and status["in_progress"]:
        while True:
            status = operator.get_job_status(run_id)
            if not status["in_progress"]:
                break

            time.sleep(wait_seconds)

    return status
