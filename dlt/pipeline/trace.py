import os
import pickle
import datetime  # noqa: 251
import dataclasses
from typing import Any, List, NamedTuple, Optional, Sequence

from dlt.common import pendulum
from dlt.common.configuration import is_secret_hint
from dlt.common.configuration.utils import _RESOLVED_TRACES

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
    step: TPipelineStep
    started_at: datetime.datetime
    finished_at: datetime.datetime
    step_info: Optional[Any]
    step_exception: Optional[str]


@dataclasses.dataclass(init=True)
class PipelineRuntimeTrace:
    engine_version: int
    resolved_config_values: List[SerializableResolvedValueTrace]
    steps: List[PipelineStepTrace]
    # extract: Optional[PipelineStepTrace] = None
    # normalize: Optional[PipelineStepTrace] = None
    # load: Optional[PipelineStepTrace] = None


def add_trace_step(trace: PipelineRuntimeTrace, step: TPipelineStep, started_at: datetime.datetime, step_info: Any) -> PipelineRuntimeTrace:
    # saves runtime trace of the pipeline
    if isinstance(step_info, PipelineStepFailed):
        step_exception = str(step_info)
        step_info = step_info.step_info
    else:
        step_info = step_info
        step_exception = None

    finished_at = pendulum.now()

    step_trace = PipelineStepTrace(step, started_at, finished_at, step_info, step_exception)
    resolved_values = map(lambda v: SerializableResolvedValueTrace(
            v.key,
            v.value,
            v.default_value,
            is_secret_hint(v.hint),
            v.namespaces,
            v.provider_name,
            str(type(v.config).__qualname__)
        ) , _RESOLVED_TRACES.values())

    trace = trace or PipelineRuntimeTrace(TRACE_ENGINE_VERSION, None, steps=[])
    trace.resolved_config_values = list(resolved_values)
    trace.steps.append(step_trace)
    # remember only last 100 steps
    trace.steps = trace.steps[-100:]
    return trace


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
