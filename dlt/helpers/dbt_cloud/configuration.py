from typing import Optional

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.typing import TSecretValue


@configspec
class DBTCloudConfiguration(BaseConfiguration):
    api_token: TSecretValue = TSecretValue("")

    account_id: Optional[str] = None
    job_id: Optional[str] = None
    project_id: Optional[str] = None
    environment_id: Optional[str] = None

    package_sha: Optional[str] = None
    package_repository_branch: Optional[str] = None
