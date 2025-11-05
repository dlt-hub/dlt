"""Implements SupportsTracking"""
from typing import Any, List, Union
from requests import Session
import fsspec
import pickle
import time
import threading
import os

from dlt.common import logger
from dlt.common.managed_thread_pool import ManagedThreadPool
from dlt.common.versioned_state import json_encode_state
from dlt.pipeline.trace import PipelineTrace, PipelineStepTrace, TPipelineStep, SupportsPipeline
from dlt.common.configuration.specs import RuntimeConfiguration
from dlt.common.schema import Schema

_THREAD_POOL: ManagedThreadPool = None
requests: Session = None

tl_store = threading.local()


def init_runtime_artifacts() -> None:
    # lazily import requests to avoid binding config before initialization
    global requests
    from dlt.sources.helpers.requests import Client

    # fail fast, don't block user
    requests = Client(request_timeout=(2, 10), request_max_attempts=0)  # type: ignore[assignment]

    global _THREAD_POOL
    if _THREAD_POOL is None:
        _THREAD_POOL = ManagedThreadPool("runtime_artifacts", 1)
        # create thread pool in controlled way, not lazy
        _THREAD_POOL._create_thread_pool()


def disable_runtime_artifacts() -> None:
    global _THREAD_POOL
    if _THREAD_POOL:
        _THREAD_POOL.stop()
    _THREAD_POOL = None


def _get_runtime_artifacts_fs(config: RuntimeConfiguration) -> fsspec.filesystem:
    if (
        not config.workspace_pipeline_artifacts_url
        or not config.workspace_artifacts_access_key
        or not config.workspace_artifacts_secret_key
        or not config.workspace_artifacts_host
    ):
        return None
    fs = fsspec.filesystem(
        "s3",
        key=config.workspace_artifacts_access_key,
        secret=config.workspace_artifacts_secret_key,
        client_kwargs={"endpoint_url": config.workspace_artifacts_host},
    )
    return fs


# write to bucket using the config, same object may be written to multiple paths
def _write_to_bucket(
    config: RuntimeConfiguration,
    pipeline_name: str,
    paths: List[str],
    data: Union[str, bytes],
    mode: str = "w",
) -> None:
    # NOTE: needs to migrate to some kind of signed urls
    fs = _get_runtime_artifacts_fs(config)

    if not fs:
        return

    for path in paths:
        with fs.open(
            f"{config.workspace_pipeline_artifacts_url}/{pipeline_name}/{path}",
            mode=mode,
        ) as f:
            f.write(data)


def _send_trace_to_bucket(trace: PipelineTrace, pipeline: SupportsPipeline) -> None:
    """
    Send the full trace pickled to the runtime bucket
    """

    def _future_send() -> None:
        try:
            pickled_trace = pickle.dumps(trace)
            _write_to_bucket(
                pipeline.run_context.runtime_config,
                pipeline.pipeline_name,
                [
                    "trace.pickle",
                    f"{tl_store.start_time}.trace.pickle",
                ],  # save current and by start time
                pickled_trace,
                mode="wb",
            )
        except Exception as e:
            logger.debug(f"Exception while sending trace to bucket: {e}")

    # _THREAD_POOL.thread_pool.submit(_future_send)
    # NOTE f3fs and futures somehow don't work here, need to investigate
    _future_send()


def _send_state_to_bucket(trace: PipelineTrace, pipeline: SupportsPipeline) -> None:
    def _future_send() -> None:
        try:
            encoded_state = json_encode_state(pipeline.state)
            _write_to_bucket(
                pipeline.run_context.runtime_config,
                pipeline.pipeline_name,
                [
                    "state.json",
                    f"{tl_store.start_time}.state.json",
                ],  # save current and by start time
                encoded_state,
                mode="w",
            )
        except Exception as e:
            logger.debug(f"Exception while sending state to bucket: {e}")

    # _THREAD_POOL.thread_pool.submit(_future_send)
    # NOTE f3fs and futures somehow don't work here, need to investigate
    _future_send()


def _send_schemas_to_bucket(pipeline: SupportsPipeline) -> None:
    schema_dir = os.path.join(pipeline.working_dir, "schemas")
    for schema_file in os.listdir(schema_dir):
        _write_to_bucket(
            pipeline.run_context.runtime_config,
            pipeline.pipeline_name,
            [f"schemas/{schema_file}"],
            open(os.path.join(schema_dir, schema_file), "rb").read(),
            mode="wb",
        )


def on_start_trace(trace: PipelineTrace, step: TPipelineStep, pipeline: SupportsPipeline) -> None:
    tl_store.start_time = time.time()


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
    pass


def on_end_trace(trace: PipelineTrace, pipeline: SupportsPipeline, send_state: bool) -> None:
    _send_trace_to_bucket(trace, pipeline)
    _send_state_to_bucket(trace, pipeline)
    _send_schemas_to_bucket(pipeline)
