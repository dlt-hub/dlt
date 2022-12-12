import contextlib
import os
import pickle
import datetime  # noqa: 251
import dataclasses
import requests
import humanize
from sentry_sdk import Hub
from sentry_sdk.tracing import Span
from typing import Any, List, NamedTuple, Optional, Sequence

from dlt.common import json, pendulum
from dlt.common import logger
from dlt.common.configuration import is_secret_hint
from dlt.common.configuration.utils import _RESOLVED_TRACES
from dlt.common.logger import _extract_github_info
from dlt.common.pipeline import LoadInfo, SupportsPipeline
from dlt.common.utils import uniq_id

from dlt.pipeline.typing import TPipelineStep
from dlt.pipeline.exceptions import PipelineStepFailed


TRACE_ENGINE_VERSION = 1
TRACE_FILE_NAME = "trace.pickle"


class SerializableResolvedValueTrace(NamedTuple):
    key: str
    value: Any
    default_value: Any
    is_secret_hint: bool
    namespaces: Sequence[str]
    provider_name: str
    config_type_name: str


@dataclasses.dataclass(init=True)
class PipelineStepTrace:
    span_id: str
    step: TPipelineStep
    started_at: datetime.datetime
    finished_at: datetime.datetime = None
    step_info: Optional[Any] = None
    step_exception: Optional[str] = None


@dataclasses.dataclass(init=True)
class PipelineRuntimeTrace:
    transaction_id: str
    started_at: datetime.datetime
    steps: List[PipelineStepTrace]
    finished_at: datetime.datetime = None
    resolved_config_values: List[SerializableResolvedValueTrace] = None
    engine_version: int = TRACE_ENGINE_VERSION


def _add_sentry_tags(span: Span, pipeline: SupportsPipeline) -> None:
    span.set_tag("pipeline_name", pipeline.pipeline_name)
    if pipeline.destination:
        span.set_tag("destination", pipeline.destination.__name__)
    if pipeline.dataset_name:
        span.set_tag("dataset_name", pipeline.dataset_name)


def _slack_notify_load(incoming_hook: str, load_info: LoadInfo, trace: PipelineRuntimeTrace) -> None:
    try:
        author = _extract_github_info().get("github_user", "")
        if author:
            author = f":hard-hat:{author}'s "

        total_elapsed = pendulum.now() - trace.started_at

        def _get_step_elapsed(step: PipelineStepTrace) -> str:
            if not step:
                return ""
            elapsed = step.finished_at - step.started_at
            return f"`{step.step.upper()}`: _{humanize.precisedelta(elapsed)}_ "

        load_step = trace.steps[-1]
        normalize_step = next((step for step in trace.steps if step.step == "normalize"), None)
        extract_step = next((step for step in trace.steps if step.step == "extract"), None)

        message = f"""The {author}pipeline *{load_info.pipeline.pipeline_name}* just loaded *{len(load_info.loads_ids)}* load package(s) to destination *{load_info.destination_name}* and into dataset *{load_info.dataset_name}*.
🚀 *{humanize.precisedelta(total_elapsed)}* of which {_get_step_elapsed(load_step)}{_get_step_elapsed(normalize_step)}{_get_step_elapsed(extract_step)}"""

        r = requests.post(incoming_hook,
            data= json.dumps({
                "text": message,
                "mrkdwn": True
                }
            ).encode("utf-8"),
            headers={'Content-Type': 'application/json;charset=utf-8'}
        )
        if r.status_code >= 400:
            logger.warning(f"Could not post the notification to slack: {r.status_code}")
    except Exception as ex:
        logger.warning(f"Slack notification could not be sent: {str(ex)}")


def start_trace(step: TPipelineStep, pipeline: SupportsPipeline) -> PipelineRuntimeTrace:
    trace = PipelineRuntimeTrace(uniq_id(), pendulum.now(), steps=[])
    # https://getsentry.github.io/sentry-python/api.html#sentry_sdk.Hub.capture_event
    if pipeline.runtime_config.sentry_dsn:
        # print(f"START SENTRY TX: {trace.transaction_id} SCOPE: {Hub.current.scope}")
        transaction = Hub.current.start_transaction(name=step, op=step)
        _add_sentry_tags(transaction, pipeline)
        transaction.__enter__()

    return trace


def start_trace_step(trace: PipelineRuntimeTrace, step: TPipelineStep, pipeline: SupportsPipeline) -> PipelineStepTrace:
    trace_step = PipelineStepTrace(uniq_id(), step, pendulum.now())
    if pipeline.runtime_config.sentry_dsn:
        # print(f"START SENTRY SPAN {trace.transaction_id}:{trace_step.span_id} SCOPE: {Hub.current.scope}")
        with contextlib.suppress(Exception):
            span = Hub.current.scope.span.start_child(description=step, op=step).__enter__()
            span.op = step
            _add_sentry_tags(span, pipeline)

    return trace_step


def end_trace_step(trace: PipelineRuntimeTrace, step: PipelineStepTrace, pipeline: SupportsPipeline, step_info: Any) -> None:
    # saves runtime trace of the pipeline
    if isinstance(step_info, PipelineStepFailed):
        step_exception = str(step_info)
        step_info = step_info.step_info
    elif isinstance(step_info, Exception):
        step_exception = str(step_info)
        step_info = None
    else:
        step_info = step_info
        step_exception = None

    step.finished_at = pendulum.now()
    step.step_exception = step_exception
    step.step_info = step_info

    resolved_values = map(lambda v: SerializableResolvedValueTrace(
            v.key,
            v.value,
            v.default_value,
            is_secret_hint(v.hint),
            v.namespaces,
            v.provider_name,
            str(type(v.config).__qualname__)
        ) , _RESOLVED_TRACES.values())

    trace.resolved_config_values = list(resolved_values)
    trace.steps.append(step)

    if pipeline.runtime_config.sentry_dsn:
        # print(f"---END SENTRY SPAN {trace.transaction_id}:{step.span_id}: {step} SCOPE: {Hub.current.scope}")
        with contextlib.suppress(Exception):
            Hub.current.scope.span.__exit__(None, None, None)
    if pipeline.runtime_config.slack_incoming_hook and step.step == "load" and step_exception is None:
        _slack_notify_load(pipeline.runtime_config.slack_incoming_hook, step_info, trace)


def end_trace(trace: PipelineRuntimeTrace, pipeline: SupportsPipeline, trace_path: str) -> None:
    if trace_path:
        save_trace(trace_path, trace)
    if pipeline.runtime_config.sentry_dsn:
        # print(f"---END SENTRY TX: {trace.transaction_id} SCOPE: {Hub.current.scope}")
        with contextlib.suppress(Exception):
            Hub.current.scope.span.__exit__(None, None, None)


def merge_traces(last_trace: PipelineRuntimeTrace, new_trace: PipelineRuntimeTrace) -> PipelineRuntimeTrace:
    """Merges `new_trace` into `last_trace` by combining steps and timestamps. `new_trace` replace the `last_trace` if it has more than 1 step.`"""
    if len(new_trace.steps) > 1 or last_trace is None:
        return new_trace

    last_trace.steps.extend(new_trace.steps)
    # remember only last 100 steps
    last_trace.steps = last_trace.steps[-100:]
    last_trace.finished_at = new_trace.finished_at
    last_trace.resolved_config_values = new_trace.resolved_config_values

    return last_trace


def save_trace(trace_path: str, trace: PipelineRuntimeTrace) -> None:
    with open(os.path.join(trace_path, TRACE_FILE_NAME), mode="bw") as f:
        f.write(pickle.dumps(trace))


def load_trace(trace_path: str) -> PipelineRuntimeTrace:
    try:
        with open(os.path.join(trace_path, TRACE_FILE_NAME), mode="br") as f:
            return pickle.load(f)  # type: ignore
    except (AttributeError, FileNotFoundError):
        # on incompatible pickling / file not found return no trace
        return None
