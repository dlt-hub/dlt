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

        This function updates a counter `name` with a value `inc`. If counter does not exist, it is
        created with optional total value of `total`. Labels are used to create sub-counters.

        Examples for counters by stage:
        ```python
        # Extract Stage
        collector.update("users", inc=5)           # 5 rows added to users table
        collector.update("Resources", inc=1)       # 1 resource processed
        collector.update("Resources", inc=1, label="Completed")  # 1 resource completed

        # Normalize Stage
        collector.update("Files", inc=1, total=10)     # 1/10 files processed
        collector.update("Items", inc=100)             # 100 items processed

        # Load Stage
        collector.update("Jobs", inc=1, total=5)       # 1/5 jobs processed
        collector.update("Jobs", inc=1, label="Failed")  # 1 job failed
        ```
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
