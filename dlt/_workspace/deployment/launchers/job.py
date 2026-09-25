"""System launcher for function-based jobs."""

from datetime import timezone
import asyncio
import inspect
from contextlib import nullcontext
from typing import Any, Callable, ContextManager, Dict, List, Optional, Tuple, Type, cast

from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs import TimezoneContext
from dlt.common.configuration.specs.base_configuration import BaseConfiguration
from dlt.common.time import to_tzinfo
from dlt.common.libs import is_instance_lib
from dlt.common.reflection.ref import object_from_ref
from dlt.common.runtime import signals
from dlt.common.time import ensure_datetime_in_tz
from dlt.common.typing import TTimeInterval

from dlt._workspace import known_sections as ws_known_sections
from dlt._workspace.deployment.decorators import JobFactory
from dlt._workspace.deployment.entity import hub_objects
from dlt._workspace.deployment.exceptions import JobAbortedException, JobResolutionError
from dlt._workspace.deployment._run_views import print_job_result
from dlt._workspace.deployment.job_result import (
    JobRunContext,
    job_inputs,
    send_job_result,
    take_job_result,
)
from dlt._workspace.deployment.typing import (
    RUN_CONTEXT_INPUT,
    THubEntity,
    TJobResult,
    TJobRunContext,
    TRuntimeEntryPoint,
    TTrigger,
    resolve_incremental_mode,
)
from dlt.extract.incremental.context import TimeIntervalContext
from dlt._workspace.deployment.launchers._launcher import (
    apply_job_configuration,
    get_run_args_port,
    parse_launcher_args,
    prepare_run_env,
    set_config_env_vars,
)


def _resolve_job(entry_point: TRuntimeEntryPoint) -> JobFactory[Any, Any]:
    """Import module and resolve the JobFactory from entry point."""
    function = entry_point.get("function")
    if not function:
        raise JobResolutionError(
            entry_point["module"],
            "entry_point.function must be set; use a module launcher for module-level jobs",
        )
    ref = f"{entry_point['module']}.{function}"

    def _typechecker(obj: Any) -> JobFactory[Any, Any]:
        if isinstance(obj, JobFactory):
            return obj
        raise JobResolutionError(ref, f"expected JobFactory, got {type(obj).__name__}")

    result, trace = object_from_ref(ref, _typechecker, raise_exec_errors=True)
    if result is None:
        raise JobResolutionError(ref, f"{trace.reason}" + (f" ({trace.exc})" if trace.exc else ""))
    return result  # type: ignore[no-any-return]


def _check_return_value(
    result: Any, job: JobFactory[Any, Any], entry_point: TRuntimeEntryPoint
) -> None:
    """Detect framework objects and delegate or raise."""
    if result is None:
        return

    # fastmcp — delegate to MCP launcher
    if is_instance_lib(result, class_ref="fastmcp.FastMCP"):
        from dlt._workspace.deployment.launchers.mcp import run_mcp_instance

        port = get_run_args_port(entry_point)
        sections = (ws_known_sections.JOBS, job.section, job.name)
        run_mcp_instance(result, port, sections)
        return

    # starlette / fastapi
    if is_instance_lib(result, class_ref="starlette.applications.Starlette"):
        raise NotImplementedError(
            f"Job returned an ASGI app ({type(result).__name__}). "
            "Use an interactive launcher with an ASGI server."
        )

    # flask
    if is_instance_lib(result, class_ref="flask.Flask"):
        raise NotImplementedError(
            f"Job returned a Flask app ({type(result).__name__}). "
            "Use an interactive launcher with a WSGI server."
        )

    # generic ASGI
    if _is_asgi_app(result):
        raise NotImplementedError(
            f"Job returned an ASGI callable ({type(result).__name__}). "
            "Use an interactive launcher with an ASGI server."
        )

    # generic WSGI
    if _is_wsgi_app(result):
        raise NotImplementedError(
            f"Job returned a WSGI callable ({type(result).__name__}). "
            "Use an interactive launcher with a WSGI server."
        )


def _is_asgi_app(obj: Any) -> bool:
    """Detect ASGI apps by checking for async __call__(scope, receive, send)."""
    if not callable(obj):
        return False
    if not asyncio.iscoroutinefunction(obj.__call__):
        return False
    params = _get_param_names(obj.__call__)
    return params is not None and len(params) == 3


def _is_wsgi_app(obj: Any) -> bool:
    """Detect WSGI apps (PEP 3333) by checking for __call__(environ, start_response)."""
    if not callable(obj):
        return False
    params = _get_param_names(obj.__call__)
    return params is not None and len(params) == 2


def _get_param_names(func: Any) -> Optional[List[str]]:
    """Get parameter names excluding self/cls. Returns None on failure."""
    import inspect as _inspect

    try:
        sig = _inspect.signature(func)
    except (ValueError, TypeError):
        return None
    return [
        p.name
        for p in sig.parameters.values()
        if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD) and p.name not in ("self", "cls")
    ]


TJobInvoke = Callable[[JobFactory[Any, Any], TJobRunContext], Any]
"""Replaces the plain job call: everything around it stays the launcher's."""


def _wants_run_context(f: Any) -> bool:
    """Check if a function declares a `run_context` parameter."""
    try:
        return RUN_CONTEXT_INPUT in inspect.signature(f).parameters
    except (ValueError, TypeError):
        return False


def job_sections(job: JobFactory[Any, Any]) -> Tuple[str, ...]:
    """Config sections of the job. A job declared outside a module has no section."""
    return tuple(p for p in (ws_known_sections.JOBS, job.section, job.name) if p)


def configured_inputs(job: JobFactory[Any, Any], spec: Type[BaseConfiguration]) -> Dict[str, Any]:
    """The job's inputs resolved as job config, so `-c`, env vars and toml all fill them."""
    config = resolve_configuration(spec(), sections=job_sections(job))
    # an input nobody supplied stays out, so a system prompt reports it as unresolved
    return {k: v for k, v in dict(config).items() if v is not None}


def _objects(job: JobFactory[Any, Any], payload: Any, values: Dict[str, Any]) -> List[THubEntity]:
    """Entities the run acted on, from the inputs it received and the payload it returned."""
    job._reflect_schemas()
    if not job.inputs and not job.output:
        return []
    return hub_objects(job.inputs, values, job.output, payload, job.job_ref)


def deliver_job_result(
    job: JobFactory[Any, Any], send: bool = True, wait: bool = False
) -> Optional[TJobResult]:
    """Finishes the run's result: type, `job_ref` and `object` filled in, then delivered.

    The one place every job kind ends up, whatever launcher ran it. `object` comes from the
    inputs the invoker recorded, or from the job's own configuration when it recorded none.
    """
    result = take_job_result(job.job_ref, job.category)
    if result is None:
        return None
    recorded = job_inputs()
    if recorded is None:
        recorded = configured_inputs(job, job._spec) if job._spec is not None else {}
    if objects := _objects(job, result.get("result"), recorded):
        result["object"] = objects
    if send:
        send_job_result(result, wait=wait)
    return result


def run(
    entry_point: TRuntimeEntryPoint,
    run_id: str,
    trigger: str,
    job: Optional[JobFactory[Any, Any]] = None,
    invoke: Optional[TJobInvoke] = None,
) -> Any:
    """Execute a function job from its entry point definition.

    Args:
        entry_point (TRuntimeEntryPoint): What to run (module + function + run_args).
        run_id (str): Unique run identifier.
        trigger (str): Trigger string that fired this run.
        job (Optional[JobFactory]): Already resolved factory, else resolved from `entry_point`.
        invoke (Optional[TJobInvoke]): Called with the job and the run context instead of
            calling the job directly.

    Returns:
        Any: The job's `TJobResult` when it declared one with `run.result`, otherwise the
        return value of the job function.
    """
    # fill unset job settings from config, then set env vars - both before user module
    # import so pipelines created at import time pick them up
    apply_job_configuration(entry_point)
    # env is emitted as well so processes the job starts inherit the run settings, the contexts
    # entered below scope the same settings in-process
    prepare_run_env(entry_point)

    job = job or _resolve_job(entry_point)
    sections = (ws_known_sections.JOBS, job.section, job.name)
    set_config_env_vars(sections, entry_point.get("config", {}))

    iv_start_str = entry_point.get("interval_start")
    iv_end_str = entry_point.get("interval_end")
    iv_tz_name = entry_point.get("interval_timezone", "UTC")
    iv: Optional[TTimeInterval] = None
    if iv_start_str and iv_end_str:
        # intervals are in UTC in transit - if user requested a different timezone
        # apply it here
        target_tz = to_tzinfo(iv_tz_name)
        iv = TTimeInterval(
            ensure_datetime_in_tz(iv_start_str, timezone.utc).astimezone(target_tz),
            ensure_datetime_in_tz(iv_end_str, timezone.utc).astimezone(target_tz),
        )

    ctx: TJobRunContext = {
        "run_id": run_id,
        "trigger": TTrigger(trigger),
        "refresh": entry_point.get("refresh", False),
    }
    run_args = entry_point.get("run_args")
    if run_args:
        ctx["run_args"] = run_args
    if iv is not None:
        ctx["interval_start"] = iv[0]
        ctx["interval_end"] = iv[1]
    # inject run_context if the function signature declares it
    kwargs: Dict[str, Any] = {"run_context": ctx} if _wants_run_context(job._f) else {}

    def _call() -> Any:
        called = invoke(job, ctx) if invoke else job(**kwargs)
        if asyncio.iscoroutine(called):
            called = asyncio.run(called)
        return called

    # default to intercepting — callers opt out with `intercept_signals=False`
    signal_ctx = (
        signals.intercepted_signals()
        if entry_point.get("intercept_signals", True)
        else nullcontext()
    )

    # the timezone context validates the declared zone before any user code runs
    tz_ctx = Container().injectable_context(TimezoneContext(iv_tz_name))
    # both branches yield different context types, which mypy cannot join without the annotation
    iv_ctx: ContextManager[Any] = nullcontext()
    if iv is not None:
        # pass True or None, False has no effect on incrementals
        iv_ctx = Container().injectable_context(
            TimeIntervalContext(
                interval=iv,
                allow_external_schedulers=(
                    resolve_incremental_mode(entry_point) == "interval" or None
                ),
            )
        )

    # TODO: job (JobFactory) should have a method that returns pipeline name on the factory
    #       then if refresh flag is set, we refresh ONLY this pipeline
    # the result context lives exactly as long as the run
    with Container().injectable_context(JobRunContext()):
        try:
            with signal_ctx, tz_ctx, iv_ctx:
                result = _call()
        except JobAbortedException:
            # an abort ends the process, so the result must be on the wire before it does
            deliver_job_result(job, wait=True)
            raise

        _check_return_value(result, job, entry_point)
        job_result = deliver_job_result(job)
    return result if job_result is None else job_result


if __name__ == "__main__":
    args = parse_launcher_args()
    # let the exception end the process
    result = run(
        entry_point=args.entry_point,
        run_id=args.run_id,
        trigger=args.trigger,
    )
    if isinstance(result, dict) and "type" in result:
        print_job_result(cast(TJobResult, result))
    elif result is not None:
        print(result)  # noqa: T201
