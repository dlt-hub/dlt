from typing import Any, Dict

from dlt.common.runtime.collector import LogCollector
from dlt.common.pipeline import SupportsPipeline
from dlt.pipeline.trace import PipelineTrace, PipelineStepTrace


def _has_schema_changes(trace: PipelineTrace) -> bool:
    """
    Check if the trace has schema changes by looking for schema updates in all load packages.
    """
    if trace is None:
        return False

    # Look at all load steps in the trace
    for step in getattr(trace, "steps", []):
        step_info = getattr(step, "step_info", None)
        if not step_info or not hasattr(step_info, "load_packages"):
            continue

        for package in step_info.load_packages:
            if hasattr(package, "schema_update") and package.schema_update:
                return True

    return False


class PlusLogCollector(LogCollector):
    """
    A minimal implementation how to do callbacks on schema changes. To implement your own callbacks
    you need to subclass this class and implement the on_schema_change method.

    Example:
    class MyLogCollector(PlusLogCollector):
        def on_schema_change(self, pipeline: SupportsPipeline, trace: PipelineTrace) -> None:
            # Custom logic for handling schema changes
            print(f"Schema change detected in step {step} of pipeline {pipeline.name}")
    """

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
            if _has_schema_changes(trace):
                self.on_schema_change(pipeline, trace)

    def on_schema_change(
        self,
        pipeline: SupportsPipeline,
        trace: PipelineTrace,
    ) -> None:
        """
        Called whenever a schema change is detected in one of the load packages during the load
        step of the pipeline.
        Args:
            pipeline: The pipeline instance
            trace: The pipeline trace
        """
        pass
