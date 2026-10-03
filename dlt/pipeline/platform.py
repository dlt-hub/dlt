"""dlthub beacon client. Also implements SupportsTracking for pipeline traces."""
from typing import Any, cast, List, Optional, TYPE_CHECKING

from dlt.common import logger
from dlt.common.json import json
from dlt.common.pipeline import LoadInfo
from dlt.common.managed_thread_pool import ManagedThreadPool
from dlt.common.schema.typing import TStoredSchema
from dlt.common.typing import TypedDict

from dlt.pipeline.trace import PipelineTrace, PipelineStepTrace, TPipelineStep, SupportsPipeline

if TYPE_CHECKING:
    from requests import Session

_THREAD_POOL: ManagedThreadPool = None
TRACE_PAYLOAD_TYPE = "trace"
STATE_PAYLOAD_TYPE = "state"
TRACE_URL_SUFFIX = f"/{TRACE_PAYLOAD_TYPE}"
STATE_URL_SUFFIX = f"/{STATE_PAYLOAD_TYPE}"
requests: "Session" = None


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
    from dlt.sources.helpers.requests import Client

    # fail fast, don't block user
    requests = Client(request_timeout=(2, 10), request_max_attempts=0)  # type: ignore[assignment]

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


def send_payload(
    payload_type: str, payload: Any, dsn: Optional[str] = None, wait: bool = False
) -> None:
    """Sends a JSON payload to the dlthub beacon. Does nothing when the beacon is not configured.

    The beacon derives the run identity from the token embedded in the DSN, so `payload` must
    not carry one.

    Args:
        payload_type (str): Beacon payload type, appended to the DSN as a path segment.
        payload (Any): JSON-serializable body.
        dsn (str): Beacon DSN. Read from the active run context when not given.
        wait (bool): Block until the request completes. Use when the process is about to exit.
    """
    if dsn is None:
        from dlt.common.runtime.run_context import active

        dsn = active().runtime_config.dlthub_dsn
    if not dsn or _THREAD_POOL is None:
        return

    url = f"{dsn}/{payload_type}"

    def _future_send() -> None:
        try:
            response = requests.put(url, data=json.dumps(payload))
            if response.status_code != 200:
                logger.debug(
                    f"Failed to send {payload_type} to platform, response code:"
                    f" {response.status_code}"
                )
        except Exception as e:
            logger.debug(f"Exception while sending {payload_type} to platform: {e}")

    future = _THREAD_POOL.thread_pool.submit(_future_send)
    if wait:
        future.result()


def _send_trace_to_platform(trace: PipelineTrace, pipeline: SupportsPipeline) -> None:
    """
    Send the full trace after a run operation to the platform
    TODO: Migrate this to open telemetry in the next iteration
    """
    dsn = pipeline.run_context.runtime_config.dlthub_dsn
    if not dsn:
        return
    send_payload(TRACE_PAYLOAD_TYPE, trace.asdict(), dsn=dsn)


def _sync_schemas_to_platform(trace: PipelineTrace, pipeline: SupportsPipeline) -> None:
    dsn = pipeline.run_context.runtime_config.dlthub_dsn
    if not dsn:
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

    send_payload(STATE_PAYLOAD_TYPE, payload, dsn=dsn)


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
