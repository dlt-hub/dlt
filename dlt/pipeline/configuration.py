from typing import Optional

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import RunConfiguration, BaseConfiguration
from dlt.common.typing import TSecretValue
from dlt.common.utils import digest256



@configspec
class PipelineConfiguration(BaseConfiguration):
    pipeline_name: Optional[str] = None
    pipelines_dir: Optional[str] = None
    destination_name: Optional[str] = None
    pipeline_salt: Optional[TSecretValue] = None
    restore_from_destination: bool = True
    enable_runtime_trace: bool = True
    runtime: RunConfiguration

    def on_resolved(self) -> None:
        if not self.pipeline_name:
            self.pipeline_name = self.runtime.pipeline_name
        if not self.pipeline_salt:
            self.pipeline_salt = TSecretValue(digest256(self.pipeline_name))
