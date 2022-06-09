from typing import Sequence
from dlt.common.exceptions import DltException
from dlt.common.telemetry import TRunMetrics
from dlt.pipeline.typing import TPipelineStage


class PipelineException(DltException):
    pass


class MissingDependencyException(PipelineException):
    def __init__(self, caller: str, dependencies: Sequence[str], appendix: str = "") -> None:
        self.caller = caller
        self.dependencies = dependencies
        super().__init__(self._get_msg(appendix))

    def _get_msg(self, appendix: str) -> str:
        msg = f"""
You must install additional dependencies to run {self.caller}. If you use pip you may do the following:

{self._to_pip_install()}
"""
        if appendix:
            msg = msg + "\n" + appendix
        return msg

    def _to_pip_install(self) -> str:
        return "\n".join([f"pip install {d}" for d in self.dependencies])


class NoPipelineException(PipelineException):
    def __init__(self) -> None:
        super().__init__("Please create or restore pipeline before using this function")


class InvalidPipelineContextException(PipelineException):
    def __init__(self) -> None:
        super().__init__("There may be just one active pipeline in single python process. You may have switch between pipelines by restoring pipeline just before using load method")


class CannotRestorePipelineException(PipelineException):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)


class PipelineStepFailed(PipelineException):
    def __init__(self, stage: TPipelineStage, exception: BaseException, run_metrics: TRunMetrics) -> None:
        self.stage = stage
        self.exception = exception
        self.run_metrics = run_metrics
        super().__init__(f"Pipeline execution failed at stage {stage} with exception:\n\n{type(exception)}\n{exception}")
