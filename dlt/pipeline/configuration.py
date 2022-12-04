from typing import Any, Optional

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import RunConfiguration, BaseConfiguration
from dlt.common.typing import AnyFun, TSecretValue
from dlt.common.utils import digest256



@configspec
class PipelineConfiguration(BaseConfiguration):
    pipeline_name: Optional[str] = None
    pipelines_dir: Optional[str] = None
    destination_name: Optional[str] = None
    pipeline_salt: Optional[TSecretValue] = None
    restore_from_destination: bool = True
    enable_runtime_trace: bool = True
    use_single_dataset: bool = True
    runtime: RunConfiguration

    def on_resolved(self) -> None:
        if not self.pipeline_name:
            self.pipeline_name = self.runtime.pipeline_name
        else:
            self.runtime.pipeline_name = self.pipeline_name
        if not self.pipeline_salt:
            self.pipeline_salt = TSecretValue(digest256(self.pipeline_name))


def ensure_correct_pipeline_kwargs(f: AnyFun, **kwargs: Any) -> None:
    for arg_name in kwargs:
        if not hasattr(PipelineConfiguration, arg_name) and not arg_name.startswith("_dlt"):
            raise TypeError(f"{f.__name__} got an unexpected keyword argument '{arg_name}'")
