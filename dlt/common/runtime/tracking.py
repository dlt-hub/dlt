from typing import Protocol, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from dlt.pipeline.trace import PipelineTrace, PipelineStepTrace
    from dlt.pipeline.typing import TPipelineStep
    from dlt.common.pipeline import SupportsPipeline
else:
    PipelineTrace = PipelineStepTrace = TPipelineStep = SupportsPipeline = Any


class SupportsTracking(Protocol):
    def on_start_trace(
        self, trace: PipelineTrace, step: TPipelineStep, pipeline: SupportsPipeline
    ) -> None: ...

    def on_start_trace_step(
        self, trace: PipelineTrace, step: TPipelineStep, pipeline: SupportsPipeline
    ) -> None: ...

    def on_end_trace_step(
        self,
        trace: PipelineTrace,
        step: PipelineStepTrace,
        pipeline: SupportsPipeline,
        step_info: Any,
        send_state: bool,
    ) -> None: ...

    def on_end_trace(
        self, trace: PipelineTrace, pipeline: SupportsPipeline, send_state: bool
    ) -> None: ...
