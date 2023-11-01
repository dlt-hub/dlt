"""Implements SupportsTracking"""
from typing import Any
import requests

from dlt.pipeline.trace import PipelineTrace, PipelineStepTrace, TPipelineStep, SupportsPipeline
from dlt.common import json

count = 0

def _send_to_beacon(trace: PipelineTrace, step: PipelineStepTrace, pipeline: SupportsPipeline, some):
    if pipeline.runtime_config.beacon_dsn:
        trace_dump = json.dumps(trace.asdict())
        requests.put(pipeline.runtime_config.beacon_dsn, data=trace_dump)

    trace_dump = json.dumps(trace.asdict(), pretty=True)
    with open(f"trace-{count}.json", "w") as f:
        f.write(trace_dump)


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

