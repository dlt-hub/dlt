import os
import pickle
import datetime  # noqa: 251
import dataclasses
from collections.abc import Sequence as C_Sequence
from typing import Any, List,  NamedTuple, Optional, Protocol, Sequence
import humanize

from dlt.common import pendulum
from dlt.common.runtime.logger import suppress_and_warn
from dlt.common.configuration import is_secret_hint
from dlt.common.configuration.utils import _RESOLVED_TRACES
from dlt.common.pipeline import ExtractDataInfo, ExtractInfo, LoadInfo, NormalizeInfo, SupportsPipeline
from dlt.common.typing import DictStrAny, StrAny
from dlt.common.utils import uniq_id

from dlt.extract.source import DltResource, DltSource
from dlt.pipeline.typing import TPipelineStep
from dlt.pipeline.exceptions import PipelineStepFailed


TRACE_ENGINE_VERSION = 1
TRACE_FILE_NAME = "trace.pickle"

# @dataclasses.dataclass(init=True)
class SerializableResolvedValueTrace(NamedTuple):
    """Information on resolved secret and config values"""
    key: str
    value: Any
    default_value: Any
    is_secret_hint: bool
    sections: Sequence[str]
    provider_name: str
    config_type_name: str

    def asdict(self) -> StrAny:
        """A dictionary representation that is safe to load."""
        return {k:v for k,v in self._asdict().items() if k not in ("value", "default_value")}

    def asstr(self, verbosity: int = 0) -> str:
        return f"{self.key}->{self.value} in {'.'.join(self.sections)} by {self.provider_name}"

    def __str__(self) -> str:
        return self.asstr(verbosity=0)


@dataclasses.dataclass(init=True)
class _PipelineStepTrace:
    span_id: str
    step: TPipelineStep
    started_at: datetime.datetime
    finished_at: datetime.datetime = None
    step_info: Optional[Any] = None
    """A step outcome info ie. LoadInfo"""
    step_exception: Optional[str] = None
    """For failing steps contains exception string"""

    def asstr(self, verbosity: int = 0) -> str:
        completed_str = "FAILED" if self.step_exception else "COMPLETED"
        if self.started_at and self.finished_at:
            elapsed = self.finished_at - self.started_at
            elapsed_str = humanize.precisedelta(elapsed)
        else:
            elapsed_str = "---"
        msg = f"Step {self.step} {completed_str} in {elapsed_str}."
        if self.step_exception:
            msg += f"\nFailed due to: {self.step_exception}"
        if self.step_info and hasattr(self.step_info, "asstr"):
            info = self.step_info.asstr(verbosity)
            if info:
                msg += f"\n{info}"
        if verbosity > 0:
            msg += f"\nspan id: {self.span_id}"
        return msg

    def __str__(self) -> str:
        return self.asstr(verbosity=0)


class PipelineStepTrace(_PipelineStepTrace):
    """Trace of particular pipeline step, contains timing information, the step outcome info or exception in case of failing step with custom asdict()"""
    def asdict(self) -> DictStrAny:
        """A dictionary representation of PipelineStepTrace that can be loaded with `dlt`"""
        d = dataclasses.asdict(self)
        if self.step_info:
            # name property depending on step name - generates nicer data
            d[f"{self.step}_info"] = d.pop("step_info")
        return d


@dataclasses.dataclass(init=True)
class PipelineTrace:
    """Pipeline runtime trace containing data on "extract", "normalize" and "load" steps and resolved config and secret values."""
    transaction_id: str
    started_at: datetime.datetime
    steps: List[PipelineStepTrace]
    """A list of steps in the trace"""
    finished_at: datetime.datetime = None
    resolved_config_values: List[SerializableResolvedValueTrace] = None
    """A list of resolved config values"""
    engine_version: int = TRACE_ENGINE_VERSION

    def asstr(self, verbosity: int = 0) -> str:
        last_step = self.steps[-1]
        completed_str = "FAILED" if last_step.step_exception else "COMPLETED"
        if self.started_at and self.finished_at:
            elapsed = self.finished_at - self.started_at
            elapsed_str = humanize.precisedelta(elapsed)
        else:
            elapsed_str = "---"
        msg = f"Run started at {self.started_at} and {completed_str} in {elapsed_str} with {len(self.steps)} steps."
        if verbosity > 0 and len(self.resolved_config_values) > 0:
            msg += "\nFollowing config and secret values were resolved:\n"
            msg += "\n".join([s.asstr(verbosity) for s in self.resolved_config_values])
            msg += "\n"
        if len(self.steps) > 0:
            msg += "\n" + "\n\n".join([s.asstr(verbosity) for s in self.steps])
        return msg

    def last_pipeline_step_trace(self, step_name: TPipelineStep) -> PipelineStepTrace:
        for step in self.steps:
            if step.step == step_name:
                return step
        return None

    @property
    def last_extract_info(self) -> ExtractInfo:
        step_trace = self.last_pipeline_step_trace("extract")
        if step_trace and isinstance(step_trace.step_info, ExtractInfo):
            return step_trace.step_info
        return None

    @property
    def last_normalize_info(self) -> NormalizeInfo:
        step_trace = self.last_pipeline_step_trace("normalize")
        if step_trace and isinstance(step_trace.step_info, NormalizeInfo):
            return step_trace.step_info
        return None

    @property
    def last_load_info(self) -> LoadInfo:
        step_trace = self.last_pipeline_step_trace("load")
        if step_trace and isinstance(step_trace.step_info, LoadInfo):
            return step_trace.step_info
        return None

    def __str__(self) -> str:
        return self.asstr(verbosity=0)


class SupportsTracking(Protocol):
    def on_start_trace(self, trace: PipelineTrace, step: TPipelineStep, pipeline: SupportsPipeline) -> None:
        ...

    def on_start_trace_step(self, trace: PipelineTrace, step: TPipelineStep, pipeline: SupportsPipeline) -> None:
        ...

    def on_end_trace_step(self, trace: PipelineTrace, step: PipelineStepTrace, pipeline: SupportsPipeline, step_info: Any) -> None:
        ...

    def on_end_trace(self, trace: PipelineTrace, pipeline: SupportsPipeline) -> None:
        ...


# plug in your own tracking module here
# TODO: that probably should be a list of modules / classes with all of them called
TRACKING_MODULE: SupportsTracking = None


def start_trace(step: TPipelineStep, pipeline: SupportsPipeline) -> PipelineTrace:
    trace = PipelineTrace(uniq_id(), pendulum.now(), steps=[])
    with suppress_and_warn():
        TRACKING_MODULE.on_start_trace(trace, step, pipeline)
    return trace


def start_trace_step(trace: PipelineTrace, step: TPipelineStep, pipeline: SupportsPipeline) -> PipelineStepTrace:
    trace_step = PipelineStepTrace(uniq_id(), step, pendulum.now())
    with suppress_and_warn():
        TRACKING_MODULE.on_start_trace_step(trace, step, pipeline)
    return trace_step


def end_trace_step(trace: PipelineTrace, step: PipelineStepTrace, pipeline: SupportsPipeline, step_info: Any) -> None:
    # saves runtime trace of the pipeline
    if isinstance(step_info, PipelineStepFailed):
        step_exception = str(step_info)
        step_info = step_info.step_info
    elif isinstance(step_info, Exception):
        step_exception = str(step_info)
        if step_info.__context__:
            step_exception += "caused by: " + str(step_info.__context__)
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


def end_trace(trace: PipelineTrace, pipeline: SupportsPipeline, trace_path: str) -> None:
    trace.finished_at = pendulum.now()
    if trace_path:
        save_trace(trace_path, trace)
    with suppress_and_warn():
        TRACKING_MODULE.on_end_trace(trace, pipeline)


def merge_traces(last_trace: PipelineTrace, new_trace: PipelineTrace) -> PipelineTrace:
    """Merges `new_trace` into `last_trace` by combining steps and timestamps. `new_trace` replace the `last_trace` if it has more than 1 step.`"""
    if len(new_trace.steps) > 1 or last_trace is None:
        return new_trace

    last_trace.steps.extend(new_trace.steps)
    # remember only last 100 steps
    last_trace.steps = last_trace.steps[-100:]
    # keep the finished up from previous trace
    last_trace.finished_at = new_trace.finished_at
    last_trace.resolved_config_values = new_trace.resolved_config_values

    return last_trace


def save_trace(trace_path: str, trace: PipelineTrace) -> None:
    with open(os.path.join(trace_path, TRACE_FILE_NAME), mode="bw") as f:
        f.write(pickle.dumps(trace))


def load_trace(trace_path: str) -> PipelineTrace:
    try:
        with open(os.path.join(trace_path, TRACE_FILE_NAME), mode="rb") as f:
            return pickle.load(f)  # type: ignore
    except (AttributeError, FileNotFoundError):
        # on incompatible pickling / file not found return no trace
        return None


def describe_extract_data(data: Any) -> List[ExtractDataInfo]:
    """Extract source and resource names from data passed to extract"""
    data_info: List[ExtractDataInfo] = []

    def add_item(item: Any) -> bool:
        if isinstance(item, (DltResource, DltSource)):
            # record names of sources/resources
            data_info.append({
                "name": item.name,
                "data_type": "resource" if isinstance(item, DltResource) else "source"
            })
            return False
        else:
            # anything else
            data_info.append({
                "name": "",
                "data_type": type(item).__name__
            })
            return True

    item: Any = data
    if isinstance(data, C_Sequence) and len(data) > 0:
        for item in data:
            # add_item returns True if non named item was returned. in that case we break
            if add_item(item):
                break
        return data_info

    add_item(item)
    return data_info
