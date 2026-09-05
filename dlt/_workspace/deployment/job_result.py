"""Structured job results and their delivery to the dlthub beacon."""

from contextlib import contextmanager
from typing import Any, ClassVar, Dict, Iterator, List, Mapping, Optional, Tuple

from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.base_configuration import (
    ContainerInjectableContext,
    configspec,
)

from dlt._workspace.deployment.exceptions import InvalidJobResultType
from dlt._workspace.deployment.typing import (
    JOB_RESULT_ENGINE_VERSION,
    JOB_RESULT_PAYLOAD_TYPE,
    TJobRef,
    TJobResult,
)


RESULT_TYPE_PREFIX = "job"
"""First segment of every result type: `job.{category}.{name}`."""


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


def result_type(category: str, name: str) -> str:
    """`job.{category}.{name}`: the category names the envelope, the name the payload."""
    return f"{RESULT_TYPE_PREFIX}.{category}.{name}"


def parse_result_type(type_: str) -> Tuple[str, str]:
    """Splits `job.{category}.{name}` into category and name. The name may contain dots.

    Raises:
        InvalidJobResultType: The string does not start with `job.` or lacks a name.
    """
    parts = type_.split(".", 2)
    if len(parts) != 3 or parts[0] != RESULT_TYPE_PREFIX or not parts[1] or not parts[2]:
        raise InvalidJobResultType(type_)
    return parts[1], parts[2]


def job_result(
    result: Any = None,
    /,
    *,
    type: str,  # noqa: A002
    engine_version: int = JOB_RESULT_ENGINE_VERSION,
) -> Any:
    """Declares the structured result of the current job run and returns `result` unchanged.

    Ignored unless the calling job is the one the launcher invoked. A job that calls another
    job as a plain function does not overwrite the run's result. A second call replaces the
    first declaration.

    Args:
        result (Any): JSON-serializable payload.
        type (str): Name of the payload shape, e.g. `"etl_summary"`. The launcher prefixes it
            with `job.{category}.` when the run finishes.
        engine_version (int): Version of that shape.

    Returns:
        Any: `result`, unchanged.
    """
    ctx = Container()[JobRunContext]
    if len(ctx.job_stack) == 1:
        ctx.result = {"type": type, "engine_version": engine_version, "result": result}
    return result


def set_job_result(result: TJobResult) -> None:
    """Declares a fully-formed result for the current top-level job. Its `type` is the bare name."""
    ctx = Container()[JobRunContext]
    if len(ctx.job_stack) <= 1:
        ctx.result = result


def set_job_inputs(inputs: Mapping[str, Any]) -> None:
    """Records the inputs the run received, so `object` can be derived from them at the end."""
    Container()[JobRunContext].inputs = dict(inputs)


def job_inputs() -> Optional[Dict[str, Any]]:
    """The inputs the run recorded, if any."""
    return Container()[JobRunContext].inputs


def take_job_result(job_ref: TJobRef, category: str) -> Optional[TJobResult]:
    """Returns and clears the declared result, with `type` and `job_ref` filled in.

    This is the one place the result type is built, so the category is always the launcher's.
    """
    ctx = Container()[JobRunContext]
    result = ctx.result
    ctx.result = None
    if result is None:
        return None
    result["type"] = result_type(category, result["type"])
    result["job_ref"] = job_ref
    return result


def send_job_result(result: TJobResult, wait: bool = False) -> None:
    """Delivers a job result to the dlthub beacon. Does nothing when it is not configured."""
    from dlt.pipeline.platform import send_payload

    send_payload(JOB_RESULT_PAYLOAD_TYPE, result, wait=wait)
