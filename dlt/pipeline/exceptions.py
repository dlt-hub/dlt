from typing import Any, Sequence
from dlt.common.exceptions import DltException, ArgumentsOverloadException
from dlt.common.telemetry import TRunMetrics
from dlt.pipeline.typing import TPipelineStep


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

class PipelineConfigMissing(PipelineException):
    def __init__(self, config_elem: str, step: TPipelineStep, _help: str = None) -> None:
        self.config_elem = config_elem
        self.step = step
        msg = f"Configuration element {config_elem} was not provided and {step} step cannot be executed"
        if _help:
            msg += f"\n{_help}\n"
        super().__init__(msg)


class CannotRestorePipelineException(PipelineException):
    def __init__(self, pipeline_name: str, working_dir: str, reason: str) -> None:
        msg = f"Pipeline with name {pipeline_name} in working directory {working_dir} could not be restored: {reason}"
        super().__init__(msg)


class SqlClientNotAvailable(PipelineException):
    def __init__(self, client_type: str) -> None:
        super().__init__(f"SQL Client not available in {client_type}")


class PipelineStepFailed(PipelineException):
    def __init__(self, step: TPipelineStep, exception: BaseException, run_metrics: TRunMetrics) -> None:
        self.stage = step
        self.exception = exception
        self.run_metrics = run_metrics
        self.step_info: Any = None
        super().__init__(f"Pipeline execution failed at stage {step} with exception:\n\n{type(exception)}\n{exception}")
