from typing import Any, Dict

from dlt.common.runtime.collector import LogCollector
from dlt.common.pipeline import SupportsPipeline
from dlt.pipeline.trace import PipelineTrace, PipelineStepTrace
from dlt.pipeline.trace_analysis import get_schema_changes


class PlusLogCollector(LogCollector):
    def on_end_trace_step(
        self,
        trace: PipelineTrace,
        step: PipelineStepTrace,
        pipeline: SupportsPipeline,
        step_info: Any,
        send_state: bool,
    ) -> None:
        # for now we only care about schema changes during the load step
        if step.step == "load":
            if schema_changes := get_schema_changes(trace, include_dlt_columns=False, include_dlt_tables=False):
                self.on_schema_change(pipeline, trace, schema_changes, step.step)

    def on_schema_change(
        self,
        pipeline: SupportsPipeline,
        trace: PipelineTrace,
        schema_changes: Dict[str, Dict[str, Any]],
    ) -> None:
        """
        Called whenever a schema change is detected in one of the load packages during the load
        step of the pipeline.
        Args:
            pipeline: The pipeline instance
            trace: The pipeline trace
            schema_changes: The combined schema changes from all load packages in the trace
        """
        pass
