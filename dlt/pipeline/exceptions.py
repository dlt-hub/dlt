from typing import Any, Dict
from dlt.common.exceptions import PipelineException
from dlt.common.pipeline import StepInfo, StepMetrics, SupportsPipeline
from dlt.pipeline.typing import TPipelineStep


class InvalidPipelineName(PipelineException, ValueError):
    def __init__(self, pipeline_name: str, details: str) -> None:
        super().__init__(
            pipeline_name,
            f"The pipeline name {pipeline_name} contains invalid characters. The pipeline name is"
            " used to create a pipeline working directory and must be a valid directory name. The"
            f" actual error is: {details}",
        )


class PipelineConfigMissing(PipelineException):
    def __init__(
        self, pipeline_name: str, config_elem: str, step: TPipelineStep, _help: str = None
    ) -> None:
        self.config_elem = config_elem
        self.step = step
        msg = (
            f"Configuration element {config_elem} was not provided and {step} step cannot be"
            " executed"
        )
        if _help:
            msg += f"\n{_help}\n"
        super().__init__(pipeline_name, msg)


class CannotRestorePipelineException(PipelineException):
    def __init__(self, pipeline_name: str, pipelines_dir: str, reason: str) -> None:
        msg = (
            f"Pipeline with name {pipeline_name} in working directory {pipelines_dir} could not be"
            f" restored: {reason}"
        )
        super().__init__(pipeline_name, msg)


class SqlClientNotAvailable(PipelineException):
    def __init__(self, pipeline_name: str, destination_name: str) -> None:
        super().__init__(
            pipeline_name,
            f"SQL Client not available for destination {destination_name} in pipeline"
            f" {pipeline_name}",
        )


class FSClientNotAvailable(PipelineException):
    def __init__(self, pipeline_name: str, destination_name: str) -> None:
        super().__init__(
            pipeline_name,
            f"Filesystem Client not available for destination {destination_name} in pipeline"
            f" {pipeline_name}",
        )


class PipelineStepFailed(PipelineException):
    """Raised by run, extract, normalize and load Pipeline methods."""

    def __init__(
        self,
        pipeline: SupportsPipeline,
        step: TPipelineStep,
        load_id: str,
        exception: BaseException,
        step_info: StepInfo[StepMetrics] = None,
    ) -> None:
        self.pipeline = pipeline
        self.step = step
        self.load_id = load_id
        self.exception = exception
        self.step_info = step_info

        package_str = f" when processing package {load_id}" if load_id else ""
        super().__init__(
            pipeline.pipeline_name,
            f"Pipeline execution failed at stage {step}{package_str} with"
            f" exception:\n\n{type(exception)}\n{exception}",
        )

    def attrs(self) -> Dict[str, Any]:
        # remove attr that should not be published
        attrs_ = super().attrs()
        attrs_.pop("pipeline")
        attrs_.pop("exception")
        attrs_.pop("step_info")
        return attrs_


class PipelineStateEngineNoUpgradePathException(PipelineException):
    def __init__(
        self, pipeline_name: str, init_engine: int, from_engine: int, to_engine: int
    ) -> None:
        self.init_engine = init_engine
        self.from_engine = from_engine
        self.to_engine = to_engine
        super().__init__(
            pipeline_name,
            f"No engine upgrade path for state in pipeline {pipeline_name} from {init_engine} to"
            f" {to_engine}, stopped at {from_engine}. You possibly tried to run an older dlt"
            " version against a destination you have previously loaded data to with a newer dlt"
            " version.",
        )


class PipelineHasPendingDataException(PipelineException):
    def __init__(self, pipeline_name: str, pipelines_dir: str) -> None:
        msg = (
            f" Operation failed because pipeline with name {pipeline_name} in working directory"
            f" {pipelines_dir} contains pending extracted files or load packages. Use `dlt pipeline"
            " sync` to reset the local state then run this operation again."
        )
        super().__init__(pipeline_name, msg)


class PipelineNeverRan(PipelineException):
    def __init__(self, pipeline_name: str, pipelines_dir: str) -> None:
        msg = (
            f" Operation failed because pipeline with name {pipeline_name} in working directory"
            f" {pipelines_dir} was never run or never synced with destination. Use `dlt pipeline"
            " sync` to synchronize."
        )
        super().__init__(pipeline_name, msg)


class PipelineNotActive(PipelineException):
    def __init__(self, pipeline_name: str) -> None:
        super().__init__(
            pipeline_name, f"Pipeline {pipeline_name} is not active so it cannot be deactivated"
        )
