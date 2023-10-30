"""Implements SupportsTracking"""
from typing import Any

from dlt.sources.helpers import requests
from dlt.pipeline.trace import PipelineTrace, PipelineStepTrace, TPipelineStep, SupportsPipeline
from dlt.common import json

count = 0

def _send_to_beacon(trace: PipelineTrace, step: PipelineStepTrace, pipeline: SupportsPipeline, some):
    if pipeline.runtime_config.beacon_token and pipeline.runtime_config.beacon_url:
        trace_dump = json.dumps(trace.asdict())
        url = f"{pipeline.runtime_config.beacon_url}/pipeline/{pipeline.runtime_config.beacon_token}/traces"
        requests.put(url, json=trace_dump)

def on_start_trace(trace: PipelineTrace, step: TPipelineStep, pipeline: SupportsPipeline) -> None:
    # _send_to_beacon(trace, step, pipeline, None)
    pass

def on_start_trace_step(trace: PipelineTrace, step: TPipelineStep, pipeline: SupportsPipeline) -> None:
    # _send_to_beacon(trace, step, pipeline, None)
    pass

def on_end_trace_step(trace: PipelineTrace, step: PipelineStepTrace, pipeline: SupportsPipeline, step_info: Any) -> None:
    # _send_to_beacon(trace, step, pipeline, step_info)
    pass

def on_end_trace(trace: PipelineTrace, pipeline: SupportsPipeline) -> None:
    _send_to_beacon(trace, None, pipeline, None)
    pass

