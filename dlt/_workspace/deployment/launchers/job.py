"""System launcher for function-based jobs.

Invocable as:
    python -m dlt._workspace.deployment.launchers.job \\
        --run-id ... --trigger ... --entry-point ...

Resolves a JobFactory via object_from_ref, injects config as env vars,
and calls it. Supports sync and async jobs.
"""

import asyncio
import os
import sys
from typing import Any, Dict, Optional

from dlt.common.reflection.ref import object_from_ref

from dlt._workspace import known_sections as ws_known_sections
from dlt._workspace.deployment.decorators import JobFactory
from dlt._workspace.deployment.typing import TEntryPoint
from dlt._workspace.deployment.launchers._launcher import parse_launcher_args


def _resolve_job(entry_point: TEntryPoint) -> JobFactory[Any, Any]:
    """Import module and resolve the JobFactory from entry point."""
    function = entry_point.get("function")
    if not function:
        raise ValueError(
            "job launcher requires entry_point.function to be set. "
            "Use a module launcher for module-level jobs."
        )
    ref = f"{entry_point['module']}.{function}"

    def _typechecker(obj: Any) -> JobFactory[Any, Any]:
        if isinstance(obj, JobFactory):
            return obj
        raise TypeError(f"expected JobFactory, got {type(obj).__name__}")

    result, trace = object_from_ref(ref, _typechecker, raise_exec_errors=True)
    if result is None:
        raise ImportError(
            f"cannot resolve job {ref!r}: {trace.reason}" + (f" ({trace.exc})" if trace.exc else "")
        )
    return result  # type: ignore[no-any-return]


def _set_config_env_vars(job: JobFactory[Any, Any], config: Dict[str, str]) -> None:
    """Set config params as env vars using the job's section and name."""
    if not config:
        return
    prefix = "__".join([ws_known_sections.JOBS, job.section, job.name]).upper()
    for key, value in config.items():
        os.environ[f"{prefix}__{key.upper()}"] = value


def _check_return_value(result: Any) -> None:
    """Detect framework objects that need specialized launchers.

    Uses isinstance checks with lazy imports so optional dependencies
    don't need to be installed. Falls back to duck typing for generic
    WSGI (PEP 3333) and ASGI protocol detection.
    """
    if result is None:
        return

    try:
        from fastmcp import FastMCP

        if isinstance(result, FastMCP):
            raise NotImplementedError(
                f"Job returned a FastMCP instance ({type(result).__name__}). "
                "Use the mcp launcher instead."
            )
    except ImportError:
        pass

    try:
        from starlette.applications import Starlette

        if isinstance(result, Starlette):
            raise NotImplementedError(
                f"Job returned an ASGI app ({type(result).__name__}). "
                "Use an interactive launcher with an ASGI server."
            )
    except ImportError:
        pass

    try:
        from flask import Flask  # type: ignore[import-not-found]

        if isinstance(result, Flask):
            raise NotImplementedError(
                f"Job returned a Flask app ({type(result).__name__}). "
                "Use an interactive launcher with a WSGI server."
            )
    except ImportError:
        pass

    if _is_asgi_app(result):
        raise NotImplementedError(
            f"Job returned an ASGI callable ({type(result).__name__}). "
            "Use an interactive launcher with an ASGI server."
        )

    if _is_wsgi_app(result):
        raise NotImplementedError(
            f"Job returned a WSGI callable ({type(result).__name__}). "
            "Use an interactive launcher with a WSGI server."
        )


def _is_asgi_app(obj: Any) -> bool:
    """Detect ASGI apps by checking for async __call__(scope, receive, send)."""
    call = getattr(obj, "__call__", None)
    if call is None or not asyncio.iscoroutinefunction(call):
        return False
    params = _get_param_names(call)
    return params is not None and len(params) == 3


def _is_wsgi_app(obj: Any) -> bool:
    """Detect WSGI apps (PEP 3333) by checking for __call__(environ, start_response)."""
    call = getattr(obj, "__call__", None)
    if call is None:
        return False
    params = _get_param_names(call)
    return params is not None and len(params) == 2


def _get_param_names(func: Any) -> Optional[list[str]]:
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


def run(
    entry_point: TEntryPoint,
    run_id: str,
    trigger: str,
    config: Optional[Dict[str, str]] = None,
) -> Any:
    """Execute a function job from its entry point definition.

    Args:
        entry_point (TEntryPoint): What to run (module + function).
        run_id (str): Unique run identifier.
        trigger (str): Trigger string that fired this run.
        config (Dict[str, str]): Config key-value pairs to inject as env vars.

    Returns:
        Any: The return value of the job function.
    """
    job = _resolve_job(entry_point)
    _set_config_env_vars(job, config or {})
    result = job()
    if asyncio.iscoroutine(result):
        result = asyncio.run(result)
    _check_return_value(result)
    return result


if __name__ == "__main__":
    args = parse_launcher_args()
    try:
        result = run(
            entry_point=args.entry_point,
            run_id=args.run_id,
            trigger=args.trigger,
            config=args.config,
        )
        if result is not None:
            print(result)
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(1)
