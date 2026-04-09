"""System launcher for function-based jobs."""

import asyncio
import inspect
import os
import sys
from typing import Any, Dict, List, Optional

from dlt.common.configuration.container import Container
from dlt.common.reflection.ref import object_from_ref
from dlt.common.time import ensure_pendulum_datetime_non_utc
from dlt.common.typing import TTimeInterval

from dlt._workspace import known_sections as ws_known_sections
from dlt._workspace._known_env import WORKSPACE__PROFILE
from dlt._workspace.deployment.decorators import JobFactory
from dlt._workspace.deployment.exceptions import JobResolutionError
from dlt._workspace.deployment.typing import TJobRunContext, TRuntimeEntryPoint, TTrigger
from dlt.extract.incremental.context import TimeIntervalContext
from dlt._workspace.deployment.launchers._launcher import (
    get_run_args_port,
    parse_launcher_args,
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
    try:
        from fastmcp import FastMCP

        if isinstance(result, FastMCP):
            from dlt._workspace.deployment.launchers.mcp import run_mcp_instance

            port = get_run_args_port(entry_point)
            sections = (ws_known_sections.JOBS, job.section, job.name)
            run_mcp_instance(result, port, sections)
            return
    except ImportError:
        pass

    # starlette / fastapi
    try:
        from starlette.applications import Starlette

        if isinstance(result, Starlette):
            raise NotImplementedError(
                f"Job returned an ASGI app ({type(result).__name__}). "
                "Use an interactive launcher with an ASGI server."
            )
    except ImportError:
        pass

    # flask
    try:
        from flask import Flask  # type: ignore[import-not-found]

        if isinstance(result, Flask):
            raise NotImplementedError(
                f"Job returned a Flask app ({type(result).__name__}). "
                "Use an interactive launcher with a WSGI server."
            )
    except ImportError:
        pass

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
    job = _resolve_job(entry_point)
    sections = (ws_known_sections.JOBS, job.section, job.name)
    set_config_env_vars(sections, entry_point.get("config", {}))

    # activate workspace profile via env var — subprocess launchers inherit it,
    # the workspace plugin reads it on init
    profile = entry_point.get("profile")
    if profile:
        os.environ[WORKSPACE__PROFILE] = profile

    # parse interval from entry point
    iv_start_str = entry_point.get("interval_start")
    iv_end_str = entry_point.get("interval_end")
    iv: Optional[TTimeInterval] = None
    if iv_start_str and iv_end_str:
        iv = (
            ensure_pendulum_datetime_non_utc(iv_start_str),
            ensure_pendulum_datetime_non_utc(iv_end_str),
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

    # inject interval context into Container so dlt.current.interval() works.
    # `allow_external_schedulers` is propagated from the manifest via entry_point —
    # the runner sets it from job_def["allow_external_schedulers"] in `_start_job`.
    if iv is not None:
        iv_ctx = TimeIntervalContext(
            interval=iv,
            allow_external_schedulers=entry_point.get("allow_external_schedulers", False),
        )
        with Container().injectable_context(iv_ctx):
            result = job(**kwargs)
            if asyncio.iscoroutine(result):
                result = asyncio.run(result)
    else:
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
