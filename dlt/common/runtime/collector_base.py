from abc import ABC, abstractmethod
from typing import Type, TypeVar, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from dlt.pipeline.trace import PipelineTrace, PipelineStepTrace
    from dlt.pipeline.typing import TPipelineStep
    from dlt.common.pipeline import SupportsPipeline
else:
    PipelineTrace = PipelineStepTrace = TPipelineStep = SupportsPipeline = Any

from dlt.common.runtime.tracking import SupportsTracking


TCollector = TypeVar("TCollector", bound="Collector")


class Collector(ABC, SupportsTracking):
    step: str

    @abstractmethod
    def update(
        self,
        name: str,
        inc: int = 1,
        total: int = None,
        inc_total: int = None,
        message: str = None,
        label: str = None,
    ) -> None:
        """Creates or updates a counter

        This function updates a counter `name` with a value `inc`. If counter does not exist, it is created with optional total value of `total`.
        Depending on implementation `label` may be used to create nested counters and message to display additional information associated with a counter.

        Args:
            name (str): An unique name of a counter, displayable.
            inc (int, optional): Increase amount. Defaults to 1.
            total (int, optional): Maximum value of a counter. Defaults to None which means unbound counter.
            icn_total (int, optional): Increase the maximum value of the counter, does nothing if counter does not exit yet
            message (str, optional): Additional message attached to a counter. Defaults to None.
            label (str, optional): Creates nested counter for counter `name`. Defaults to None.
        """
        pass

    @abstractmethod
    def _start(self, step: str) -> None:
        """Starts counting for a processing step with name `step`"""
        pass

    @abstractmethod
    def _stop(self) -> None:
        """Stops counting. Should close all counters and release resources ie. screen or push the results to a server."""
        pass

    def __call__(self: TCollector, step: str) -> TCollector:
        """Syntactic sugar for nicer context managers"""
        self.step = step
        return self

    def __enter__(self: TCollector) -> TCollector:
        self._start(self.step)
        return self

    def __exit__(self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: Any) -> None:
        self._stop()

    def on_start_trace(
        self, trace: PipelineTrace, step: TPipelineStep, pipeline: SupportsPipeline
    ) -> None:
        pass

    def on_start_trace_step(
        self, trace: PipelineTrace, step: TPipelineStep, pipeline: SupportsPipeline
    ) -> None:
        pass

    def on_end_trace_step(
        self,
        trace: PipelineTrace,
        step: PipelineStepTrace,
        pipeline: SupportsPipeline,
        step_info: Any,
        send_state: bool,
    ) -> None:
        pass

    def on_end_trace(
        self, trace: PipelineTrace, pipeline: SupportsPipeline, send_state: bool
    ) -> None:
        pass
