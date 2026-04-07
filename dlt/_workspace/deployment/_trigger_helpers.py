import re
from fnmatch import fnmatch
from typing import Callable, Dict, List, Optional, Sequence, Tuple, Union
from urllib.parse import urlparse

from dlt.common.time import ensure_pendulum_datetime_utc
from dlt.common.typing import TAnyDateTime
from dlt._workspace.deployment._job_ref import resolve_job_ref
from dlt._workspace.deployment.exceptions import InvalidTrigger
from dlt._workspace.deployment.typing import (
    HttpTriggerInfo,
    TJobDefinition,
    TParsedTrigger,
    TTrigger,
    TTriggerType,
)


_PERIOD_MULTIPLIERS = {"s": 1, "m": 60, "h": 3600, "d": 86400}
_PERIOD_RE = re.compile(r"^(\d+(?:\.\d+)?)\s*([smhd])$")


def parse_period_seconds(value: str) -> float:
    """Parse a human period string (e.g. '5m', '1h', '30s') into seconds.

    Also accepts bare numeric strings as seconds.

    Raises:
        ValueError: If the string cannot be parsed.
    """
    match = _PERIOD_RE.match(value.strip())
    if match:
        return float(match.group(1)) * _PERIOD_MULTIPLIERS[match.group(2)]
    return float(value)


def _parse_schedule(expr: str) -> TParsedTrigger:
    if not expr:
        raise InvalidTrigger(f"schedule:{expr}", "requires a cron expression")
    from croniter import croniter

    if not croniter.is_valid(expr):
        raise InvalidTrigger(f"schedule:{expr}", "invalid cron expression")
    return TParsedTrigger(type="schedule", expr=expr, raw=TTrigger(f"schedule:{expr}"))


def _parse_every(expr: str) -> TParsedTrigger:
    if not expr:
        raise InvalidTrigger(f"every:{expr}", "requires a period (e.g. '5m', '1h')")
    try:
        seconds = parse_period_seconds(expr)
    except ValueError:
        raise InvalidTrigger(f"every:{expr}", "period must be like '5m', '1h', '1d'")
    if seconds <= 0:
        raise InvalidTrigger(f"every:{expr}", "period must be positive")
    if seconds < 60:
        raise InvalidTrigger(f"every:{expr}", "minimum period is 1 minute")
    return TParsedTrigger(type="every", expr=seconds, raw=TTrigger(f"every:{expr}"))


def _parse_once(expr: str) -> TParsedTrigger:
    if not expr:
        raise InvalidTrigger(f"once:{expr}", "requires a timestamp (ISO 8601)")
    try:
        dt = ensure_pendulum_datetime_utc(expr)
    except (ValueError, TypeError):
        raise InvalidTrigger(f"once:{expr}", "requires ISO 8601 timestamp")
    return TParsedTrigger(type="once", expr=dt, raw=TTrigger(f"once:{expr}"))


def _parse_http(expr: str) -> TParsedTrigger:
    if not expr:
        return TParsedTrigger(type="http", expr=HttpTriggerInfo(None, ""), raw=TTrigger("http:"))

    if expr.startswith("//") or expr.startswith("http"):
        raise InvalidTrigger(f"http:{expr}", "should be http:[port][/path], not a full URL")

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
            raise InvalidTrigger(f"http:{expr}", f"port invalid: {e}")
        if url_port is not None:
            if url_port < 1:
                raise InvalidTrigger(f"http:{expr}", f"port must be 1-65535, got {url_port}")
            port = url_port
        else:
            raise InvalidTrigger(f"http:{expr}", "expr must be [port][/path]")

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
        raise InvalidTrigger("tag:", "requires a name")
    return TParsedTrigger(type="tag", expr=expr, raw=TTrigger(f"tag:{expr}"))


def _parse_manual(expr: str) -> TParsedTrigger:
    return TParsedTrigger(type="manual", expr=expr or None, raw=TTrigger(f"manual:{expr}"))


def _parse_job_success(expr: str) -> TParsedTrigger:
    if not expr:
        raise InvalidTrigger("job.success:", "requires a job_ref")
    if not expr.startswith("jobs."):
        raise InvalidTrigger(f"job.success:{expr}", "expression must start with 'jobs.'")
    return TParsedTrigger(type="job.success", expr=expr, raw=TTrigger(f"job.success:{expr}"))


def _parse_job_fail(expr: str) -> TParsedTrigger:
    if not expr:
        raise InvalidTrigger("job.fail:", "requires a job_ref")
    if not expr.startswith("jobs."):
        raise InvalidTrigger(f"job.fail:{expr}", "expression must start with 'jobs.'")
    return TParsedTrigger(type="job.fail", expr=expr, raw=TTrigger(f"job.fail:{expr}"))


def _parse_pipeline_name(expr: str) -> TParsedTrigger:
    if not expr:
        raise InvalidTrigger("pipeline_name:", "requires a pipeline name")
    return TParsedTrigger(type="pipeline_name", expr=expr, raw=TTrigger(f"pipeline_name:{expr}"))


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
    "pipeline_name": _parse_pipeline_name,
}


def parse_trigger(trigger: TTrigger) -> TParsedTrigger:
    """Parse a normalized trigger string into a structured TParsedTrigger."""
    if ":" not in trigger:
        raise InvalidTrigger(trigger, "must be in type:expr form")
    trigger_type, expr = trigger.split(":", 1)
    parser = PARSERS.get(trigger_type)
    if parser is None:
        raise InvalidTrigger(trigger, f"unknown type {trigger_type!r}")
    return parser(expr)


def normalize_trigger(trigger: Union[str, TTrigger]) -> TTrigger:
    """Normalize a single user-provided trigger to canonical form.

    Raises InvalidTrigger for ``manual:`` and ``pipeline_name:`` triggers which are
    synthetic (added automatically from expose and deliver specs).
    """
    _SYNTHETIC_TYPES = {"manual", "pipeline_name"}

    # import here to avoid circular import — creators live in triggers.py
    from dlt._workspace.deployment.trigger import schedule

    s = str(trigger).strip()

    if ":" in s:
        trigger_type = s.split(":", 1)[0]
        if trigger_type in _SYNTHETIC_TYPES:
            raise InvalidTrigger(s, f"{trigger_type}: triggers are added automatically")
        if trigger_type in PARSERS:
            return parse_trigger(TTrigger(s)).raw
        raise InvalidTrigger(s, f"unknown type {trigger_type!r}")

    if s in PARSERS:
        if s in _SYNTHETIC_TYPES:
            raise InvalidTrigger(s, f"{s}: triggers are added automatically")
        if s in ("deployment", "http", "webhook"):
            return PARSERS[s]("").raw
        raise InvalidTrigger(s, "requires an expression")

    # detect bare cron expressions
    from dlt._workspace.deployment.interval import is_cron_expression

    if is_cron_expression(s):
        return schedule(s)

    raise InvalidTrigger(s, "cannot normalize")


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
_SELECTOR_KEYWORDS = _JOB_TYPE_SELECTORS | set(PARSERS.keys())


def is_selector(s: str) -> bool:
    """Check if string looks like a trigger selector vs a bare job ref."""
    if ":" in s:
        return True
    return s in _SELECTOR_KEYWORDS


def _normalize_selector(selector: str) -> str:
    """Expand shorthand selectors to glob form."""
    if selector in PARSERS:
        return f"{selector}:*"
    if ":" in selector and selector.endswith(":"):
        return f"{selector}*"
    return selector


def match_triggers_with_selectors(
    job_type: str,
    triggers: List[TTrigger],
    selectors: List[str],
) -> List[TTrigger]:
    """Return triggers that match any selector.

    Job-type selectors (batch, interactive, stream, job) match ALL triggers.
    """
    matched: List[TTrigger] = []

    for selector in selectors:
        if selector in _JOB_TYPE_SELECTORS:
            if (selector == "job" and job_type == "batch") or job_type == selector:
                return list(triggers)
            continue

        pattern = _normalize_selector(selector)
        for t in triggers:
            if t not in matched and fnmatch(t, pattern):
                matched.append(t)

    return matched


def pick_trigger(
    matched: List[TTrigger],
    default_trigger: Optional[TTrigger] = None,
) -> Optional[TTrigger]:
    """Pick one trigger from a matched list. Prefer default_trigger if present."""
    if not matched:
        return None
    if default_trigger and default_trigger in matched:
        return default_trigger
    return matched[0]


def maybe_parse_schedule(job_def: TJobDefinition) -> Optional[str]:
    """Extract the cron expression from a job's schedule trigger.

    Returns:
        The cron expression string if the job has a `schedule:` trigger,
        or `None` if no schedule trigger is found.
    """
    for trigger in job_def.get("triggers", []):
        try:
            parsed = parse_trigger(trigger)
        except InvalidTrigger:
            continue
        if parsed.type == "schedule":
            return str(parsed.expr)
    return None
