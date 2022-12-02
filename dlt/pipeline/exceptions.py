from typing import Any, Sequence
from dlt.common.exceptions import DltException
from dlt.common.pipeline import SupportsPipeline
from dlt.common.telemetry import TRunMetrics
from dlt.pipeline.typing import TPipelineStep


class PipelineException(DltException):
    def __init__(self, pipeline_name: str, msg: str) -> None:
        self.pipeline_name = pipeline_name
        super().__init__(msg)


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
    def __init__(self, pipeline: SupportsPipeline, step: TPipelineStep, exception: BaseException, run_metrics: TRunMetrics, step_info: Any = None) -> None:
        self.pipeline = pipeline
        self.stage = step
        self.exception = exception
        self.run_metrics = run_metrics
        self.step_info = step_info
        super().__init__(pipeline.pipeline_name, f"Pipeline execution failed at stage {step} with exception:\n\n{type(exception)}\n{exception}")


class PipelineStateNotAvailable(PipelineException):
    def __init__(self, source_name: str) -> None:
        if source_name:
            msg = f"The source {source_name} requested the access to pipeline state but no pipeline is active right now."
        else:
            msg = "The resource you called requested the access to pipeline state but no pipeline is active right now."
        msg += " Call dlt.pipeline(...) before you call the @dlt.source or  @dlt.resource decorated function."
        self.source_name = source_name
        super().__init__(None, msg)


class PipelineStateEngineNoUpgradePathException(PipelineException):
    def __init__(self, pipeline_name: str, init_engine: int, from_engine: int, to_engine: int) -> None:
        self.init_engine = init_engine
        self.from_engine = from_engine
        self.to_engine = to_engine
        super().__init__(pipeline_name, f"No engine upgrade path for state in pipeline {pipeline_name} from {init_engine} to {to_engine}, stopped at {from_engine}")
