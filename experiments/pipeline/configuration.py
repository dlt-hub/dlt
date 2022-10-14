from typing import Optional

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import RunConfiguration, BaseConfiguration

from dlt.common.typing import TSecretValue
from dlt.common.utils import uniq_id


@configspec
class PipelineConfiguration(BaseConfiguration):
    working_dir: Optional[str] = None
    pipeline_secret: Optional[TSecretValue] = None
    runtime: RunConfiguration

    def check_integrity(self) -> None:
        if self.pipeline_secret:
            self.pipeline_secret = uniq_id()
