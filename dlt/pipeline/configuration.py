from typing import Any, Optional

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import RunConfiguration, BaseConfiguration
from dlt.common.typing import AnyFun, TSecretValue
from dlt.common.utils import digest256
from dlt.common.data_writers import TLoaderFileFormat


@configspec
class PipelineConfiguration(BaseConfiguration):
    pipeline_name: Optional[str] = None
    pipelines_dir: Optional[str] = None
    destination_type: Optional[str] = None
    destination_name: Optional[str] = None
    staging_type: Optional[str] = None
    staging_name: Optional[str] = None
    loader_file_format: Optional[TLoaderFileFormat] = None
    dataset_name: Optional[str] = None
    pipeline_salt: Optional[TSecretValue] = None
    restore_from_destination: bool = True
    """Enables the `run` method of the `Pipeline` object to restore the pipeline state and schemas from the destination"""
    enable_runtime_trace: bool = True
    """Enables the tracing. Tracing saves the execution trace locally and is required by `dlt deploy`."""
    use_single_dataset: bool = True
    """Stores all schemas in single dataset. When False, each schema will get a separate dataset with `{dataset_name}_{schema_name}"""
    full_refresh: bool = False
    """When set to True, each instance of the pipeline with the `pipeline_name` starts from scratch when run and loads the data to a separate dataset."""
    progress: Optional[str] = None
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
