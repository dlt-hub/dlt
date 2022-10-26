from typing import Any, Sequence
from dlt.common.exceptions import DltException, ArgumentsOverloadException
from dlt.common.telemetry import TRunMetrics
from experiments.pipeline.typing import TPipelineStep


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


class PipelineConfigMissing(PipelineException):
    def __init__(self, config_elem: str, step: TPipelineStep, help: str = None) -> None:
        self.config_elem = config_elem
        self.step = step
        msg = f"Configuration element {config_elem} was not provided and {step} step cannot be executed"
        if help:
            msg += f"\n{help}\n"
        super().__init__(msg)


# class PipelineConfiguredException(PipelineException):
#     def __init__(self, f_name: str) -> None:
#         super().__init__(f"{f_name} cannot be called on already configured or restored pipeline.")


# class InvalidPipelineContextException(PipelineException):
#     def __init__(self) -> None:
#         super().__init__("There may be just one active pipeline in single python process. To activate current pipeline call `activate` method")


class CannotRestorePipelineException(PipelineException):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)


class SqlClientNotAvailable(PipelineException):
    def __init__(self, client_type: str) -> None:
        super().__init__(f"SQL Client not available in {client_type}")


class InvalidIteratorException(PipelineException):
    def __init__(self, iterator: Any) -> None:
        super().__init__(f"Unsupported source iterator or iterable type: {type(iterator).__name__}")


class InvalidItemException(PipelineException):
    def __init__(self, item: Any) -> None:
        super().__init__(f"Source yielded unsupported item type: {type(item).__name}. Only dictionaries, sequences and deferred items allowed.")


class PipelineStepFailed(PipelineException):
    def __init__(self, step: TPipelineStep, exception: BaseException, run_metrics: TRunMetrics) -> None:
        self.stage = step
        self.exception = exception
        self.run_metrics = run_metrics
        super().__init__(f"Pipeline execution failed at stage {step} with exception:\n\n{type(exception)}\n{exception}")


# class CannotApplyHintsToManyResources(ArgumentsOverloadException):
#     pass
