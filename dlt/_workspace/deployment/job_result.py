"""Structured job results and their delivery to the dlthub beacon."""

from contextlib import contextmanager
from typing import Any, ClassVar, Dict, Iterator, List, Mapping, Optional, cast

from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.base_configuration import (
    ContainerInjectableContext,
    configspec,
)

from dlt._workspace.deployment.typing import (
    BACKGROUND_AGENT_CATEGORY,
    JOB_RESULT_CATEGORY,
    JOB_RESULT_ENGINE_VERSION,
    JOB_RESULT_PAYLOAD_TYPE,
    TJobRef,
    TJobResult,
    TJobResultCategory,
)


@configspec
class JobRunContext(ContainerInjectableContext):
    """Job call stack of the current thread and the result declared by its top-level job."""

    can_create_default: ClassVar[bool] = True
    global_affinity: ClassVar[bool] = False

    def __init__(self) -> None:
        super().__init__()
        self.job_stack: List[TJobRef] = []
        self.result: Optional[TJobResult] = None
        self.inputs: Optional[Dict[str, Any]] = None


@contextmanager
def running_job(job_ref: TJobRef) -> Iterator[None]:
    """Marks `job_ref` as running. The outermost job on the stack owns the run result."""
    ctx = Container()[JobRunContext]
    ctx.job_stack.append(job_ref)
    try:
        yield
    finally:
        ctx.job_stack.pop()


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
    ctx = Container()[JobRunContext]
    # a job called by another job as a plain function does not overwrite the run's result
    if len(ctx.job_stack) == 1:
        # `take_job_result` fills in the type when none was declared
        declared = cast(TJobResult, {"engine_version": engine_version, "result": result})
        if type:
            declared["type"] = type
        ctx.result = declared
    return result


def set_job_result(result: TJobResult) -> None:
    """Declares a fully-formed result for the current top-level job. Its `type` is the bare name."""
    ctx = Container()[JobRunContext]
    if len(ctx.job_stack) <= 1:
        ctx.result = result


def set_job_inputs(inputs: Mapping[str, Any]) -> None:
    """Records the run's inputs; `TJobResult.object` is derived from them."""
    Container()[JobRunContext].inputs = dict(inputs)


def job_inputs() -> Optional[Dict[str, Any]]:
    """The inputs the run recorded, if any."""
    return Container()[JobRunContext].inputs


def take_job_result(
    job_ref: TJobRef, category: TJobResultCategory, name: str
) -> Optional[TJobResult]:
    """Returns and clears the declared result, with `type` and `job_ref` filled in."""
    ctx = Container()[JobRunContext]
    result = ctx.result
    ctx.result = None
    if result is None:
        return None
    # declared name wins over the job's own `name`
    result["type"] = result_type(category, result.get("type") or name)
    result["job_ref"] = job_ref
    return result


def send_job_result(result: TJobResult, wait: bool = False) -> None:
    """Delivers a job result to the dlthub beacon. Does nothing when it is not configured."""
    from dlt.pipeline.platform import send_payload

    send_payload(JOB_RESULT_PAYLOAD_TYPE, result, wait=wait)
