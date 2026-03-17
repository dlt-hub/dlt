import re
from fnmatch import fnmatch
from typing import Callable, Dict, List, Optional, Sequence, Tuple, Union
from urllib.parse import urlparse

from dlt.common.time import ensure_pendulum_datetime_utc
from dlt.common.typing import TAnyDateTime
from dlt._workspace.deployment._job_ref import resolve_job_ref
from dlt._workspace.deployment.typing import (
    HttpTriggerInfo,
    TJobDefinition,
    TParsedTrigger,
    TTrigger,
    TTriggerType,
)


_PERIOD_MULTIPLIERS = {"s": 1, "m": 60, "h": 3600, "d": 86400}
_PERIOD_RE = re.compile(r"^(\d+(?:\.\d+)?)\s*([smhd])$")


def _parse_schedule(expr: str) -> TParsedTrigger:
    if not expr:
        raise ValueError("schedule trigger requires a cron expression")
    return TParsedTrigger(type="schedule", expr=expr, raw=TTrigger(f"schedule:{expr}"))


def _parse_every(expr: str) -> TParsedTrigger:
    if not expr:
        raise ValueError("every trigger requires a period (e.g. '5h', '30m', '10s')")
    match = _PERIOD_RE.match(expr.strip())
    if match:
        seconds = float(match.group(1)) * _PERIOD_MULTIPLIERS[match.group(2)]
    else:
        try:
            seconds = float(expr)
        except ValueError:
            raise ValueError(f"every trigger period must be like '5h', '30m', '10s', got {expr!r}")
    if seconds <= 0:
        raise ValueError(f"every trigger period must be positive, got {expr!r}")
    return TParsedTrigger(type="every", expr=seconds, raw=TTrigger(f"every:{expr}"))


def _parse_once(expr: str) -> TParsedTrigger:
    if not expr:
        raise ValueError("once trigger requires a timestamp (ISO 8601)")
    try:
        dt = ensure_pendulum_datetime_utc(expr)
    except (ValueError, TypeError):
        raise ValueError(f"once trigger requires ISO 8601 timestamp, got {expr!r}")
    return TParsedTrigger(type="once", expr=dt, raw=TTrigger(f"once:{expr}"))


def _parse_http(expr: str) -> TParsedTrigger:
    if not expr:
        return TParsedTrigger(type="http", expr=HttpTriggerInfo(None, ""), raw=TTrigger("http:"))

    if expr.startswith("//") or expr.startswith("http"):
        raise ValueError("http trigger should be http:[port][/path], not a full URL")

    if expr[0] == "/":
        synthetic = f"http://localhost{expr}"
    else:
        synthetic = f"http://localhost:{expr}"

    parsed = urlparse(synthetic)
    port: Optional[int] = None
    path = parsed.path or ""

    if expr[0] != "/":
        try:
            url_port = parsed.port
        except ValueError as e:
            raise ValueError(f"http trigger port invalid: {e}")
        if url_port is not None:
            if url_port < 1:
                raise ValueError(f"http trigger port must be 1-65535, got {url_port}")
            port = url_port
        else:
            raise ValueError(f"http trigger expr must be [port][/path], got {expr!r}")

    if port is not None and path == "/" and "/" not in expr[len(str(port)) :]:
        path = ""

    return TParsedTrigger(
        type="http", expr=HttpTriggerInfo(port, path), raw=TTrigger(f"http:{expr}")
    )


def _parse_deployment(expr: str) -> TParsedTrigger:
    return TParsedTrigger(type="deployment", expr=expr or None, raw=TTrigger(f"deployment:{expr}"))


def _parse_webhook(expr: str) -> TParsedTrigger:
    return TParsedTrigger(type="webhook", expr=expr or None, raw=TTrigger(f"webhook:{expr}"))


def _parse_tag(expr: str) -> TParsedTrigger:
    if not expr:
        raise ValueError("tag trigger requires a name")
    return TParsedTrigger(type="tag", expr=expr, raw=TTrigger(f"tag:{expr}"))


def _parse_manual(expr: str) -> TParsedTrigger:
    return TParsedTrigger(type="manual", expr=expr or None, raw=TTrigger(f"manual:{expr}"))


def _parse_job_success(expr: str) -> TParsedTrigger:
    if not expr:
        raise ValueError("job.success trigger requires a job_ref")
    if not expr.startswith("jobs."):
        raise ValueError(f"job.success expression must start with 'jobs.', got {expr!r}")
    return TParsedTrigger(type="job.success", expr=expr, raw=TTrigger(f"job.success:{expr}"))


def _parse_job_fail(expr: str) -> TParsedTrigger:
    if not expr:
        raise ValueError("job.fail trigger requires a job_ref")
    if not expr.startswith("jobs."):
        raise ValueError(f"job.fail expression must start with 'jobs.', got {expr!r}")
    return TParsedTrigger(type="job.fail", expr=expr, raw=TTrigger(f"job.fail:{expr}"))


PARSERS: Dict[str, Callable[[str], TParsedTrigger]] = {
    "schedule": _parse_schedule,
    "every": _parse_every,
    "once": _parse_once,
    "http": _parse_http,
    "deployment": _parse_deployment,
    "webhook": _parse_webhook,
    "tag": _parse_tag,
    "manual": _parse_manual,
    "job.success": _parse_job_success,
    "job.fail": _parse_job_fail,
}


def parse_trigger(trigger: TTrigger) -> TParsedTrigger:
    """Parse a normalized trigger string into a structured TParsedTrigger."""
    if ":" not in trigger:
        raise ValueError(f"trigger must be in type:expr form, got {trigger!r}")
    trigger_type, expr = trigger.split(":", 1)
    parser = PARSERS.get(trigger_type)
    if parser is None:
        raise ValueError(f"unknown trigger type {trigger_type!r} in {trigger!r}")
    return parser(expr)


_CRON_FIELD_RE = re.compile(r"^[\d*,/\-?LW#]+$")


def _looks_like_cron(s: str) -> bool:
    """Check if string is a valid cron expression."""
    try:
        from croniter import croniter

        return croniter.is_valid(s)
    except ImportError:
        # fallback: 5 or 6 fields with cron-like characters
        parts = s.split()
        return len(parts) in (5, 6) and all(_CRON_FIELD_RE.match(p) for p in parts)


def normalize_trigger(trigger: Union[str, TTrigger]) -> TTrigger:
    """Normalize a single trigger to canonical form."""
    # import here to avoid circular import — creators live in triggers.py
    from dlt._workspace.deployment.triggers import schedule

    s = str(trigger).strip()

    if ":" in s:
        trigger_type = s.split(":", 1)[0]
        if trigger_type in PARSERS:
            return parse_trigger(TTrigger(s)).raw
        raise ValueError(f"unknown trigger type {trigger_type!r} in {s!r}")

    if s in PARSERS:
        if s in ("deployment", "manual", "http", "webhook"):
            return PARSERS[s]("").raw
        raise ValueError(f"trigger type {s!r} requires an expression")

    # detect bare cron expressions
    if _looks_like_cron(s):
        return schedule(s)

    raise ValueError(f"cannot normalize trigger {trigger!r}")


def normalize_triggers(
    trigger: Union[
        None, str, TTrigger, Sequence[Union[str, TTrigger]], Tuple[Union[str, TTrigger], ...]
    ],
) -> List[TTrigger]:
    """Normalize trigger input to a list of canonical TTrigger values."""
    if trigger is None:
        return []
    if isinstance(trigger, tuple):
        items: List[Union[str, TTrigger]] = []
        for t in trigger:
            if isinstance(t, tuple):
                items.extend(t)
            else:
                items.append(t)
        return [normalize_trigger(t) for t in items]
    if isinstance(trigger, str):
        trigger = [trigger]
    return [normalize_trigger(t) for t in trigger]


_JOB_TYPE_SELECTORS = {"batch", "interactive", "stream", "job"}
_TRIGGER_TYPE_NAMES = set(PARSERS.keys())


def matches_selector(selector: str, job_def: TJobDefinition) -> bool:
    """Check if a job definition matches a selector."""
    if selector in _JOB_TYPE_SELECTORS:
        job_type = job_def["entry_point"]["job_type"]
        if selector == "job":
            return job_type == "batch"
        return job_type == selector

    triggers = job_def.get("triggers", [])

    if selector in _TRIGGER_TYPE_NAMES:
        selector = f"{selector}:*"
    elif ":" in selector and selector.endswith(":"):
        selector = f"{selector}*"

    return any(fnmatch(t, selector) for t in triggers)


def filter_jobs_by_selectors(
    jobs: List[TJobDefinition], selectors: List[str]
) -> List[TJobDefinition]:
    """Filter jobs matching any of the selectors. Empty = all."""
    if not selectors:
        return list(jobs)
    return [j for j in jobs if any(matches_selector(s, j) for s in selectors)]
