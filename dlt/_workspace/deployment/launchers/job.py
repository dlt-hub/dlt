"""System launcher for function-based jobs."""

from datetime import timezone
import asyncio
import inspect
from contextlib import nullcontext
from typing import Any, ContextManager, Dict, List, Optional

from dlt.common.configuration.container import Container
from dlt.common.configuration.specs import TimezoneContext
from dlt.common.time import to_tzinfo
from dlt.common.libs import is_instance_lib
from dlt.common.reflection.ref import object_from_ref
from dlt.common.runtime import signals
from dlt.common.time import ensure_datetime_in_tz
from dlt.common.typing import TTimeInterval

from dlt._workspace import known_sections as ws_known_sections
from dlt._workspace.deployment.decorators import JobFactory
from dlt._workspace.deployment.exceptions import JobResolutionError
from dlt._workspace.deployment.typing import (
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


def _wants_run_context(f: Any) -> bool:
    """Check if a function declares a `run_context` parameter."""
    try:
        return "run_context" in inspect.signature(f).parameters
    except (ValueError, TypeError):
        return False


def run(
    entry_point: TRuntimeEntryPoint,
    run_id: str,
    trigger: str,
) -> Any:
    """Execute a function job from its entry point definition.

    Args:
        entry_point (TRuntimeEntryPoint): What to run (module + function + run_args).
        run_id (str): Unique run identifier.
        trigger (str): Trigger string that fired this run.

    Returns:
        Any: The return value of the job function.
    """
    # fill unset job settings from config, then set env vars - both before user module
    # import so pipelines created at import time pick them up
    apply_job_configuration(entry_point, entry_point.get("function"))
    # env is emitted as well so processes the job starts inherit the run settings, the contexts
    # entered below scope the same settings in-process
    prepare_run_env(entry_point)

    job = _resolve_job(entry_point)
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

    # inject run_context if the function signature declares it
    kwargs: Dict[str, Any] = {}
    if _wants_run_context(job._f):
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
        kwargs["run_context"] = ctx

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
    with signal_ctx, tz_ctx, iv_ctx:
        result = job(**kwargs)
        if asyncio.iscoroutine(result):
            result = asyncio.run(result)

    _check_return_value(result, job, entry_point)
    return result


if __name__ == "__main__":
    args = parse_launcher_args()
    # let the exception end the process
    result = run(
        entry_point=args.entry_point,
        run_id=args.run_id,
        trigger=args.trigger,
    )
    if result is not None:
        print(result)  # noqa: T201
