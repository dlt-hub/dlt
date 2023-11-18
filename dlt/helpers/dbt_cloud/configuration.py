from typing import Optional

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.typing import TSecretValue


@configspec
class DBTCloudConfiguration(BaseConfiguration):
    api_token: TSecretValue = TSecretValue("")

    account_id: Optional[str]
    job_id: Optional[str]
    project_id: Optional[str]
    environment_id: Optional[str]
    run_id: Optional[str]

    cause: str = "Triggered via API"
    git_sha: Optional[str]
    git_branch: Optional[str]
    schema_override: Optional[str]
