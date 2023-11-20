"""Implements SupportsTracking"""
from typing import Any
import requests
from concurrent.futures import ThreadPoolExecutor

from dlt.pipeline.trace import PipelineTrace, PipelineStepTrace, TPipelineStep, SupportsPipeline
from dlt.common import json
from dlt.common.runtime import logger

_THREAD_POOL: ThreadPoolExecutor = None

def _init_thread_pool_if_needed() -> None:
    global _THREAD_POOL
    if not _THREAD_POOL:
        _THREAD_POOL = ThreadPoolExecutor(1)

def _send_to_platform(trace: PipelineTrace, pipeline: SupportsPipeline) -> None:
    if pipeline.runtime_config.platform_dsn:

        def _future_send() -> None:
            trace_dump = json.dumps(trace.asdict())
            response = requests.put(pipeline.runtime_config.platform_dsn, data=trace_dump)
            if response.status_code != 200:
                logger.debug("Failed to send trace to platform.")

        _init_thread_pool_if_needed()
        _THREAD_POOL.submit(_future_send)

        # trace_dump = json.dumps(trace.asdict(), pretty=True)
        # with open(f"trace.json", "w") as f:
        #     f.write(trace_dump)

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
    _send_to_platform(trace, pipeline)

