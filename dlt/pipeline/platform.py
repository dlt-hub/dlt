"""Implements SupportsTracking"""
from typing import Any, cast, List
from requests import Session

from dlt.common import logger
from dlt.common.json import json
from dlt.common.pipeline import LoadInfo
from dlt.common.managed_thread_pool import ManagedThreadPool
from dlt.common.schema.typing import TStoredSchema
from dlt.common.typing import TypedDict

from dlt.pipeline.trace import PipelineTrace, PipelineStepTrace, TPipelineStep, SupportsPipeline

_THREAD_POOL: ManagedThreadPool = None
TRACE_URL_SUFFIX = "/trace"
STATE_URL_SUFFIX = "/state"
requests: Session = None


class TPipelineSyncPayload(TypedDict):
    pipeline_name: str
    destination_name: str
    destination_displayable_credentials: str
    destination_fingerprint: str
    dataset_name: str
    schemas: List[TStoredSchema]


def init_platform_tracker() -> None:
    # lazily import requests to avoid binding config before initialization
    global requests
    from dlt.sources.helpers import requests as r_

    requests = r_  # type: ignore[assignment]

    global _THREAD_POOL
    if _THREAD_POOL is None:
        _THREAD_POOL = ManagedThreadPool("platform_tracker", 1)
        # create thread pool in controlled way, not lazy
        _THREAD_POOL._create_thread_pool()


def disable_platform_tracker() -> None:
    global _THREAD_POOL
    if _THREAD_POOL:
        _THREAD_POOL.stop()
    _THREAD_POOL = None


def _send_trace_to_platform(trace: PipelineTrace, pipeline: SupportsPipeline) -> None:
    """
    Send the full trace after a run operation to the platform
    TODO: Migrate this to open telemetry in the next iteration
    """
    if not pipeline.runtime_config.dlthub_dsn:
        return

    def _future_send() -> None:
        try:
            trace_dump = json.dumps(trace.asdict())
            url = pipeline.runtime_config.dlthub_dsn + TRACE_URL_SUFFIX
            response = requests.put(url, data=trace_dump)
            if response.status_code != 200:
                logger.debug(
                    f"Failed to send trace to platform, response code: {response.status_code}"
                )
        except Exception as e:
            logger.debug(f"Exception while sending trace to platform: {e}")

    _THREAD_POOL.thread_pool.submit(_future_send)

    # trace_dump = json.dumps(trace.asdict(), pretty=True)
    # with open(f"trace.json", "w") as f:
    #     f.write(trace_dump)


def _sync_schemas_to_platform(trace: PipelineTrace, pipeline: SupportsPipeline) -> None:
    if not pipeline.runtime_config.dlthub_dsn:
        return

    # sync only if load step was processed
    load_info: LoadInfo = None
    for step in trace.steps:
        if step.step == "load":
            load_info = cast(LoadInfo, step.step_info)

    if not load_info:
        return

    payload = TPipelineSyncPayload(
        pipeline_name=pipeline.pipeline_name,
        destination_name=load_info.destination_name,
        destination_displayable_credentials=load_info.destination_displayable_credentials,
        destination_fingerprint=load_info.destination_fingerprint,
        dataset_name=load_info.dataset_name,
        schemas=[],
    )

    # attach all schemas
    for schema_name in pipeline.schemas:
        schema = pipeline.schemas[schema_name]
        payload["schemas"].append(schema.to_dict())

    def _future_send() -> None:
        try:
            url = pipeline.runtime_config.dlthub_dsn + STATE_URL_SUFFIX
            response = requests.put(url, data=json.dumps(payload))
            if response.status_code != 200:
                logger.debug(
                    f"Failed to send state to platform, response code: {response.status_code}"
                )
        except Exception as e:
            logger.debug(f"Exception while sending state to platform: {e}")

    _THREAD_POOL.thread_pool.submit(_future_send)


def on_start_trace(trace: PipelineTrace, step: TPipelineStep, pipeline: SupportsPipeline) -> None:
    pass


def on_start_trace_step(
    trace: PipelineTrace, step: TPipelineStep, pipeline: SupportsPipeline
) -> None:
    pass


def on_end_trace_step(
    trace: PipelineTrace,
    step: PipelineStepTrace,
    pipeline: SupportsPipeline,
    step_info: Any,
    send_state: bool,
) -> None:
    if send_state:
        # also sync schemas to dlthub
        _sync_schemas_to_platform(trace, pipeline)


def on_end_trace(trace: PipelineTrace, pipeline: SupportsPipeline, send_state: bool) -> None:
    _send_trace_to_platform(trace, pipeline)
