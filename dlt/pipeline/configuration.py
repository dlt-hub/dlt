from typing import Optional

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import RunConfiguration, BaseConfiguration
from dlt.common.typing import TSecretValue
from dlt.common.utils import uniq_id



@configspec
class PipelineConfiguration(BaseConfiguration):
    pipeline_name: Optional[str] = None
    working_dir: Optional[str] = None
    pipeline_secret: Optional[TSecretValue] = None
    restore_from_destination: bool = False
    _runtime: RunConfiguration

    def on_resolved(self) -> None:
        if not self.pipeline_secret:
            self.pipeline_secret = TSecretValue(uniq_id())
        if not self.pipeline_name:
            self.pipeline_name = self._runtime.pipeline_name
