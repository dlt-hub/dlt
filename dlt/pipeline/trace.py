import contextlib
from copy import copy
import os
import pickle
import datetime  # noqa: 251
import dataclasses
from typing import Any, List, NamedTuple, Optional, Protocol, Sequence
import humanize

from dlt.common import pendulum, json
from dlt.common.configuration import is_secret_hint
from dlt.common.configuration.exceptions import ContextDefaultCannotBeCreated
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.configuration.utils import _RESOLVED_TRACES
from dlt.common.configuration.container import Container
from dlt.common.exceptions import ExceptionTrace, ResourceNameNotAvailable
from dlt.common.runtime.logger import suppress_and_warn
from dlt.common.runtime.exec_info import TExecutionContext, get_execution_context
from dlt.common.pipeline import (
    ExtractInfo,
    LoadInfo,
    NormalizeInfo,
    PipelineContext,
    StepInfo,
    StepMetrics,
    SupportsPipeline,
)
from dlt.common.source import get_current_pipe_name
from dlt.common.typing import DictStrAny, StrAny, SupportsHumanize
from dlt.common.utils import uniq_id, get_exception_trace_chain

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
        return {k: v for k, v in self._asdict().items() if k not in ("value", "default_value")}

    def asstr(self, verbosity: int = 0) -> str:
        return f"{self.key}->{self.value} in {'.'.join(self.sections)} by {self.provider_name}"

    def __str__(self) -> str:
        return self.asstr(verbosity=0)


class _PipelineStepTrace(NamedTuple):
    span_id: str
    step: TPipelineStep
    started_at: datetime.datetime
    finished_at: datetime.datetime = None
    step_info: Optional[StepInfo[StepMetrics]] = None
    """A step outcome info ie. LoadInfo"""
    step_exception: Optional[str] = None
    """For failing steps contains exception string"""
    exception_traces: List[ExceptionTrace] = None
    """For failing steps contains traces of exception chain causing it"""


class PipelineStepTrace(SupportsHumanize, _PipelineStepTrace):
    """Trace of particular pipeline step, contains timing information, the step outcome info or exception in case of failing step with custom asdict()"""

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

    def asdict(self) -> DictStrAny:
        """A dictionary representation of PipelineStepTrace that can be loaded with `dlt`"""
        d = self._asdict()
        if self.step_info:
            # name property depending on step name - generates nicer data
            d[f"{self.step}_info"] = step_info_dict = d.pop("step_info").asdict()
            d["step_info"] = {}
            # take only the base keys
            for prop in self.step_info._astuple()._asdict():
                d["step_info"][prop] = step_info_dict.pop(prop)
        # replace the attributes in exception traces with json dumps
        if self.exception_traces:
            # do not modify original traces
            d["exception_traces"] = copy(d["exception_traces"])
            traces: List[ExceptionTrace] = d["exception_traces"]
            for idx in range(len(traces)):
                if traces[idx].get("exception_attrs"):
                    # trace: ExceptionTrace
                    trace = traces[idx] = copy(traces[idx])
                    trace["exception_attrs"] = str(trace["exception_attrs"])  # type: ignore[typeddict-item]

        return d

    def __str__(self) -> str:
        return self.asstr(verbosity=0)


class _PipelineTrace(NamedTuple):
    """Pipeline runtime trace containing data on "extract", "normalize" and "load" steps and resolved config and secret values."""

    transaction_id: str
    pipeline_name: str
    execution_context: TExecutionContext
    started_at: datetime.datetime
    steps: List[PipelineStepTrace]
    """A list of steps in the trace"""
    finished_at: datetime.datetime = None
    resolved_config_values: List[SerializableResolvedValueTrace] = None
    """A list of resolved config values"""
    engine_version: int = TRACE_ENGINE_VERSION


class PipelineTrace(SupportsHumanize, _PipelineTrace):
    def asstr(self, verbosity: int = 0) -> str:
        last_step = self.steps[-1]
        completed_str = "FAILED" if last_step.step_exception else "COMPLETED"
        if self.started_at and self.finished_at:
            elapsed = self.finished_at - self.started_at
            elapsed_str = humanize.precisedelta(elapsed)
        else:
            elapsed_str = "---"
        msg = (
            f"Run started at {self.started_at} and {completed_str} in {elapsed_str} with"
            f" {len(self.steps)} steps."
        )
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

    def asdict(self) -> DictStrAny:
        """A dictionary representation of PipelineTrace that can be loaded with `dlt`"""
        d = self._asdict()
        # run step is the same as load step
        d["steps"] = [step.asdict() for step in self.steps]  # if step.step != "run"
        return d

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
    def on_start_trace(
        self, trace: PipelineTrace, step: TPipelineStep, pipeline: SupportsPipeline
    ) -> None: ...

    def on_start_trace_step(
        self, trace: PipelineTrace, step: TPipelineStep, pipeline: SupportsPipeline
    ) -> None: ...

    def on_end_trace_step(
        self,
        trace: PipelineTrace,
        step: PipelineStepTrace,
        pipeline: SupportsPipeline,
        step_info: Any,
        send_state: bool,
    ) -> None: ...

    def on_end_trace(
        self, trace: PipelineTrace, pipeline: SupportsPipeline, send_state: bool
    ) -> None: ...


# plug in your own tracking modules here
TRACKING_MODULES: List[SupportsTracking] = None


def start_trace(step: TPipelineStep, pipeline: SupportsPipeline) -> PipelineTrace:
    trace = PipelineTrace(
        uniq_id(),
        pipeline.pipeline_name,
        get_execution_context(),
        pendulum.now(),
        steps=[],
        resolved_config_values=[],
    )
    for module in TRACKING_MODULES:
        with suppress_and_warn():
            module.on_start_trace(trace, step, pipeline)
    return trace


def start_trace_step(
    trace: PipelineTrace, step: TPipelineStep, pipeline: SupportsPipeline
) -> PipelineStepTrace:
    trace_step = PipelineStepTrace(uniq_id(), step, pendulum.now())
    for module in TRACKING_MODULES:
        with suppress_and_warn():
            module.on_start_trace_step(trace, step, pipeline)
    return trace_step


def end_trace_step(
    trace: PipelineTrace,
    step: PipelineStepTrace,
    pipeline: SupportsPipeline,
    step_info: Any,
    send_state: bool,
) -> PipelineTrace:
    # saves runtime trace of the pipeline
    if isinstance(step_info, PipelineStepFailed):
        exception_traces = get_exception_traces(step_info)
        step_exception = str(step_info)
        step_info = step_info.step_info
    elif isinstance(step_info, Exception):
        exception_traces = get_exception_traces(step_info)
        step_exception = str(step_info)
        if step_info.__context__:
            step_exception += "caused by: " + str(step_info.__context__)
        step_info = None
    else:
        step_info = step_info
        exception_traces = None
        step_exception = None

    step = step._replace(
        finished_at=pendulum.now(),
        step_exception=step_exception,
        exception_traces=exception_traces,
        step_info=step_info,
    )
    resolved_values = map(
        lambda v: SerializableResolvedValueTrace(
            v.key,
            v.value,
            v.default_value,
            is_secret_hint(v.hint),
            v.sections,
            v.provider_name,
            str(type(v.config).__qualname__),
        ),
        _RESOLVED_TRACES.values(),
    )

    trace.resolved_config_values[:] = list(resolved_values)
    trace.steps.append(step)
    for module in TRACKING_MODULES:
        with suppress_and_warn():
            module.on_end_trace_step(trace, step, pipeline, step_info, send_state)
    return trace


def end_trace(
    trace: PipelineTrace, pipeline: SupportsPipeline, trace_path: str, send_state: bool
) -> PipelineTrace:
    trace = trace._replace(finished_at=pendulum.now())
    if trace_path:
        save_trace(trace_path, trace)
    for module in TRACKING_MODULES:
        with suppress_and_warn():
            module.on_end_trace(trace, pipeline, send_state)
    return trace


def merge_traces(last_trace: PipelineTrace, new_trace: PipelineTrace) -> PipelineTrace:
    """Merges `new_trace` into `last_trace` by combining steps and timestamps. `new_trace` replace the `last_trace` if it has more than 1 step.`"""
    if len(new_trace.steps) > 1 or last_trace is None:
        return new_trace

    last_trace.steps.extend(new_trace.steps)
    # remember only last 100 steps and keep the finished up from previous trace
    return last_trace._replace(
        steps=last_trace.steps[-100:],
        finished_at=new_trace.finished_at,
        resolved_config_values=new_trace.resolved_config_values,
    )


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


def get_exception_traces(exc: BaseException, container: Container = None) -> List[ExceptionTrace]:
    """Gets exception trace chain and extend it with data available in Container context"""
    traces = get_exception_trace_chain(exc)
    container = container or Container()

    # get resource name
    resource_name: str = None
    with contextlib.suppress(ResourceNameNotAvailable):
        resource_name = get_current_pipe_name()
    # get source name
    source_name: str = None
    with contextlib.suppress(ContextDefaultCannotBeCreated):
        sections_context = container[ConfigSectionContext]
        source_name = sections_context.source_state_key
    # get pipeline name
    proxy = container[PipelineContext]
    if proxy.is_active():
        pipeline_name = proxy.pipeline().pipeline_name
    else:
        pipeline_name = None

    # apply context to trace
    for trace in traces:
        # only to dlt exceptions
        if "exception_attrs" in trace:
            trace.setdefault("resource_name", resource_name)
            trace.setdefault("pipeline_name", pipeline_name)
            trace.setdefault("source_name", source_name)

    return traces
