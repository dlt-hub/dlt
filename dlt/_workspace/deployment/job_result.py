"""Structured job results and their delivery to the dlthub beacon."""

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, ClassVar, Dict, Iterator, List, Optional, cast

from dlt._workspace.deployment.typing import (
    BACKGROUND_AGENT_CATEGORY,
    JOB_RESULT_CATEGORY,
    JOB_RESULT_ENGINE_VERSION,
    JOB_RESULT_PAYLOAD_TYPE,
    TJobRef,
    TJobResult,
    TJobResultCategory,
)


class JobRun:
    """Job call stack of a run and the result declared by its top-level job."""

    # a context variable, not a thread local: concurrent asyncio tasks each run their own job
    _current: ClassVar[ContextVar[Optional["JobRun"]]] = ContextVar("current_job_run", default=None)

    def __init__(self) -> None:
        self.job_stack: List[TJobRef] = []
        self.result: Optional[TJobResult] = None
        self.inputs: Optional[Dict[str, Any]] = None

    @classmethod
    def current(cls) -> Optional["JobRun"]:
        return cls._current.get()

    @classmethod
    def active(cls) -> "JobRun":
        """The current run, for code that only runs inside one."""
        run = cls._current.get()
        assert run is not None, "no job run is active"
        return run

    @classmethod
    @contextmanager
    def running(cls, job_ref: TJobRef) -> Iterator["JobRun"]:
        """Marks `job_ref` as running. The outermost job on the stack owns the run result."""
        run = cls._current.get()
        if run is None:
            # a job called outside of any run starts its own
            with JobRun().activate():
                with cls.running(job_ref) as run:
                    yield run
            return
        run.job_stack.append(job_ref)
        try:
            yield run
        finally:
            run.job_stack.pop()

    @contextmanager
    def activate(self) -> Iterator["JobRun"]:
        """Makes this the current run of the calling thread or task."""
        token = JobRun._current.set(self)
        try:
            yield self
        finally:
            JobRun._current.reset(token)

    def declare_result(
        self, result: Any, type: Optional[str], engine_version: int  # noqa: A002
    ) -> None:
        """Records the result `run.result` declared, when the top-level job declared it."""
        # a job called by another job as a plain function does not overwrite the run's result
        if len(self.job_stack) != 1:
            return
        # `take_result` fills in the type when none was declared
        declared = cast(TJobResult, {"engine_version": engine_version, "result": result})
        if type:
            declared["type"] = type
        self.result = declared

    def set_result(self, result: TJobResult) -> None:
        """Sets a fully-formed result for the top-level job. Its `type` is the bare name."""
        # an agent without a function puts no job on the stack
        if len(self.job_stack) <= 1:
            self.result = result

    def take_result(
        self, job_ref: TJobRef, category: TJobResultCategory, name: str
    ) -> Optional[TJobResult]:
        """Returns and clears the declared result, with `type` and `job_ref` filled in."""
        result, self.result = self.result, None
        if result is None:
            return None
        # declared name wins over the job's own `name`
        result["type"] = result_type(category, result.get("type") or name)
        result["job_ref"] = job_ref
        return result


def result_type(category: TJobResultCategory, name: str) -> str:
    """`job.{name}` for a job, `job.background_agent.{name}` for an agent job."""
    prefix = f"{JOB_RESULT_CATEGORY}."
    if category != JOB_RESULT_CATEGORY:
        name = f"{category}.{name}"
    # a name that already starts with `job.` is not prefixed again
    return name if name.startswith(prefix) else prefix + name


def is_job_result(value: Any) -> bool:
    """Whether `value` is a job result: a dict whose `type` starts with `job.`."""
    if not isinstance(value, dict):
        return False
    type_ = value.get("type")
    return isinstance(type_, str) and type_.startswith(f"{JOB_RESULT_CATEGORY}.")


def is_agent_result(type_: str) -> bool:
    """Whether a result type is that of an agent job."""
    return type_.startswith(result_type(BACKGROUND_AGENT_CATEGORY, ""))


def job_result(
    result: Any = None,
    /,
    *,
    type: Optional[str] = None,  # noqa: A002
    engine_version: int = JOB_RESULT_ENGINE_VERSION,
) -> Any:
    """Declares the structured result of the current job run and returns `result` unchanged.

    Only the job the launcher invoked declares the result; a second call replaces the first.

    Args:
        result (Any): JSON-serializable payload.
        type (Optional[str]): Name of the payload shape, e.g. `"etl_summary"`. Defaults to job name.
        engine_version (int): Version of that shape.

    Returns:
        Any: `result`, unchanged.
    """
    run = JobRun.current()
    # outside a job the payload still comes back, it is simply not recorded
    if run is not None:
        run.declare_result(result, type, engine_version)
    return result


def send_job_result(result: TJobResult, wait: bool = False) -> None:
    """Delivers a job result to the dlthub beacon. Does nothing when it is not configured."""
    from dlt.pipeline.platform import send_payload

    send_payload(JOB_RESULT_PAYLOAD_TYPE, result, wait=wait)
