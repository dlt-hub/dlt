from typing import Optional, Union

from dlt.common.time import ensure_pendulum_datetime_utc
from dlt.common.typing import TAnyDateTime
from dlt._workspace.deployment._job_ref import resolve_job_ref
from dlt._workspace.deployment._trigger_helpers import (
    _parse_deployment,
    _parse_every,
    _parse_http,
    _parse_manual,
    _parse_once,
    _parse_schedule,
    _parse_tag,
    _parse_webhook,
    _parse_job_fail,
    _parse_job_success,
)
from dlt._workspace.deployment.typing import TTrigger

__all__ = [
    "schedule",
    "every",
    "once",
    "http",
    "deployment",
    "webhook",
    "tag",
    "manual",
    "job_success",
    "job_fail",
]


def schedule(cron_expr: str) -> TTrigger:
    """Create a cron schedule trigger."""
    return _parse_schedule(cron_expr.strip() if cron_expr else "").raw


def every(period: Union[str, float]) -> TTrigger:
    """Create a recurring interval trigger. Accepts `"5h"` or seconds (float)."""
    if isinstance(period, (int, float)):
        return _parse_every(f"{period}s").raw
    return _parse_every(period.strip() if period else "").raw


def once(at: TAnyDateTime) -> TTrigger:
    """Create a one-shot trigger. Accepts ISO string, datetime, date, or unix timestamp."""
    try:
        dt = ensure_pendulum_datetime_utc(at)
    except (ValueError, TypeError):
        raise ValueError(f"once expects a valid date/time value, got {at!r}")
    return _parse_once(dt.isoformat()).raw


def http(port: Optional[int] = None, path: Optional[str] = None) -> TTrigger:
    """Create an HTTP trigger for interactive jobs."""
    if port is not None and not (1 <= port <= 65535):
        raise ValueError(f"http trigger port must be 1-65535, got {port}")
    if path is not None and path and not path.startswith("/"):
        raise ValueError(f"http trigger path must start with '/', got {path!r}")
    expr = ""
    if port is not None:
        expr = str(port)
    if path:
        expr += path
    return _parse_http(expr).raw


def deployment() -> TTrigger:
    """Create a deployment trigger (fires after code deploy)."""
    return _parse_deployment("").raw


def webhook(path: str = "") -> TTrigger:
    """Create a webhook trigger."""
    return _parse_webhook(path).raw


def tag(name: str) -> TTrigger:
    """Create a tag broadcast trigger."""
    return _parse_tag(name.strip() if name else "").raw


def manual(job_ref: str = "") -> TTrigger:
    """Create a manual trigger."""
    return _parse_manual(job_ref).raw


def job_success(job_ref: str) -> TTrigger:
    """Create a job success event trigger.

    Args:
        job_ref: Job reference — accepts `"name"` (2-part), `"section.name"`,
            or `"jobs.section.name"`.
    """
    return _parse_job_success(resolve_job_ref(job_ref)).raw


def job_fail(job_ref: str) -> TTrigger:
    """Create a job failure event trigger.

    Args:
        job_ref: Job reference — accepts `"name"` (2-part), `"section.name"`,
            or `"jobs.section.name"`.
    """
    return _parse_job_fail(resolve_job_ref(job_ref)).raw
