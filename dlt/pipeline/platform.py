"""Implements SupportsTracking"""
from dlt.pipeline.trace import PipelineTrace, PipelineStepTrace, TPipelineStep, SupportsPipeline
from typing import Any


def _send_to_beacon(trace: PipelineTrace, step: PipelineStepTrace, pipeline: SupportsPipeline):
    if not pipeline.runtime_config.beacon_token or not pipeline.runtime_config.beacon_url:
        return

def on_start_trace(trace: PipelineTrace, step: TPipelineStep, pipeline: SupportsPipeline) -> None:
    _send_to_beacon(trace, step, pipeline, None)

def on_start_trace_step(trace: PipelineTrace, step: TPipelineStep, pipeline: SupportsPipeline) -> None:
    _send_to_beacon(trace, step, pipeline, None)

def on_end_trace_step(trace: PipelineTrace, step: PipelineStepTrace, pipeline: SupportsPipeline, step_info: Any) -> None:
    _send_to_beacon(trace, step, pipeline, step_info)

def on_end_trace(trace: PipelineTrace, pipeline: SupportsPipeline) -> None:
    _send_to_beacon(trace, None, pipeline, None)


