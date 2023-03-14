import contextlib
import os
import pickle
import datetime  # noqa: 251
import dataclasses
from typing import Any, List, NamedTuple, Optional, Protocol, Sequence

from dlt.common import pendulum
from dlt.common.runtime.logger import suppress_and_warn
from dlt.common.configuration import is_secret_hint
from dlt.common.configuration.utils import _RESOLVED_TRACES
from dlt.common.runtime.exec_info import github_info
from dlt.common.runtime.segment import track as dlthub_telemetry_track
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
    sections: Sequence[str]
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


class SupportsTracking(Protocol):
    def on_start_trace(self, trace: PipelineRuntimeTrace, step: TPipelineStep, pipeline: SupportsPipeline) -> None:
        ...

    def on_start_trace_step(self, trace: PipelineRuntimeTrace, step: TPipelineStep, pipeline: SupportsPipeline) -> None:
        ...

    def on_end_trace_step(self, trace: PipelineRuntimeTrace, step: PipelineStepTrace, pipeline: SupportsPipeline, step_info: Any) -> None:
        ...

    def on_end_trace(self, trace: PipelineRuntimeTrace, pipeline: SupportsPipeline) -> None:
        ...


# plug in your own tracking module here
# TODO: that probably should be a list of modules / classes with all of them called
TRACKING_MODULE: SupportsTracking = None


def start_trace(step: TPipelineStep, pipeline: SupportsPipeline) -> PipelineRuntimeTrace:
    trace = PipelineRuntimeTrace(uniq_id(), pendulum.now(), steps=[])
    with suppress_and_warn():
        TRACKING_MODULE.on_start_trace(trace, step, pipeline)
    return trace


def start_trace_step(trace: PipelineRuntimeTrace, step: TPipelineStep, pipeline: SupportsPipeline) -> PipelineStepTrace:
    trace_step = PipelineStepTrace(uniq_id(), step, pendulum.now())
    with suppress_and_warn():
        TRACKING_MODULE.on_start_trace_step(trace, step, pipeline)
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
            v.sections,
            v.provider_name,
            str(type(v.config).__qualname__)
        ) , _RESOLVED_TRACES.values())

    trace.resolved_config_values = list(resolved_values)
    trace.steps.append(step)
    with suppress_and_warn():
        TRACKING_MODULE.on_end_trace_step(trace, step, pipeline, step_info)


def end_trace(trace: PipelineRuntimeTrace, pipeline: SupportsPipeline, trace_path: str) -> None:
    if trace_path:
        save_trace(trace_path, trace)
    with suppress_and_warn():
        TRACKING_MODULE.on_end_trace(trace, pipeline)


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
