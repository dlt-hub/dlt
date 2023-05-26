from typing import Any
from dlt.common.exceptions import PipelineException
from dlt.common.pipeline import SupportsPipeline
from dlt.pipeline.typing import TPipelineStep


class InvalidPipelineName(PipelineException, ValueError):
    def __init__(self, pipeline_name: str, details: str) -> None:
        super().__init__(pipeline_name, f"The pipeline name {pipeline_name} contains invalid characters. The pipeline name is used to create a pipeline working directory and must be a valid directory name. The actual error is: {details}")


class PipelineConfigMissing(PipelineException):
    def __init__(self, pipeline_name: str, config_elem: str, step: TPipelineStep, _help: str = None) -> None:
        self.config_elem = config_elem
        self.step = step
        msg = f"Configuration element {config_elem} was not provided and {step} step cannot be executed"
        if _help:
            msg += f"\n{_help}\n"
        super().__init__(pipeline_name, msg)


class CannotRestorePipelineException(PipelineException):
    def __init__(self, pipeline_name: str, pipelines_dir: str, reason: str) -> None:
        msg = f"Pipeline with name {pipeline_name} in working directory {pipelines_dir} could not be restored: {reason}"
        super().__init__(pipeline_name, msg)


class SqlClientNotAvailable(PipelineException):
    def __init__(self, pipeline_name: str,destination_name: str) -> None:
        super().__init__(pipeline_name, f"SQL Client not available for destination {destination_name} in pipeline {pipeline_name}")


class PipelineStepFailed(PipelineException):
    def __init__(self, pipeline: SupportsPipeline, step: TPipelineStep, exception: BaseException, step_info: Any = None) -> None:
        self.pipeline = pipeline
        self.step = step
        self.exception = exception
        self.step_info = step_info
        super().__init__(pipeline.pipeline_name, f"Pipeline execution failed at stage {step} with exception:\n\n{type(exception)}\n{exception}")


class PipelineStateEngineNoUpgradePathException(PipelineException):
    def __init__(self, pipeline_name: str, init_engine: int, from_engine: int, to_engine: int) -> None:
        self.init_engine = init_engine
        self.from_engine = from_engine
        self.to_engine = to_engine
        super().__init__(pipeline_name, f"No engine upgrade path for state in pipeline {pipeline_name} from {init_engine} to {to_engine}, stopped at {from_engine}")


class PipelineHasPendingDataException(PipelineException):
    def __init__(self, pipeline_name: str, pipelines_dir: str) -> None:
        msg = (
            f" Operation failed because pipeline with name {pipeline_name} in working directory {pipelines_dir} contains pending extracted files or load packages. "
            "Use `dlt pipeline sync` to reset the local state then run this operation again."
        )
        super().__init__(pipeline_name, msg)


class PipelineNotActive(PipelineException):
    def __init__(self, pipeline_name: str) -> None:
        super().__init__(pipeline_name, f"Pipeline {pipeline_name} is not active so it cannot be deactivated")
