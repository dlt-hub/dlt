"""Interval computation and run-based upstream freshness checks."""

from datetime import datetime, timedelta, timezone  # noqa: I251
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from dlt import version
from dlt.common.exceptions import MissingDependencyException
from dlt.common.time import ensure_datetime_in_tz, ensure_datetime_utc
from dlt.common.typing import TTimeInterval

from dlt._workspace.deployment._trigger_helpers import (
    maybe_parse_schedule,
    parse_trigger,
)
from dlt._workspace.deployment.freshness import parse_freshness_constraint
from dlt._workspace.deployment.exceptions import InvalidTrigger
from dlt._workspace.deployment.typing import (
    TFreshnessConstraint,
    TIntervalSpec,
    TJobDefinition,
    TTrigger,
)

try:
    from croniter import croniter
except ModuleNotFoundError:
    raise MissingDependencyException(
        "dltHub workspace",
        [f"{version.DLT_PKG_NAME}[workspace]"],
    )


# disable interval based freshness checks
INTERVAL_FRESHNESS_ENABLED: bool = False


def next_scheduled_run(
    trigger: TTrigger,
    now_reference: datetime,
    tz: str = "UTC",
    prev_scheduled_run: Optional[datetime] = None,
) -> datetime:
    """Compute the next scheduled run for a timed trigger.

    Returns the UTC datetime when the job should next run.

    Args:
        trigger: A ``schedule:``, ``every:``, or ``once:`` trigger.
        now_reference: UTC reference time.
        tz: IANA timezone for cron evaluation.
        prev_scheduled_run: When the previous run was scheduled (for ``every:``).

    Raises:
        InvalidTrigger: If trigger is not a timed type.
    """
    parsed = parse_trigger(trigger)
    tt = parsed.type
    now_ref = ensure_datetime_utc(now_reference)

    if tt == "schedule":
        cron_expr = str(parsed.expr)
        target_tz = ZoneInfo(tz)
        # iterate cron in naive local time then attach target_tz — stdlib croniter
        # with aware ZoneInfo datetimes mishandles DST fall-back (naive math would
        # yield wall-clock 09:00 instead of 08:00). Naive iteration sidesteps that.
        now_local_naive = now_ref.astimezone(target_tz).replace(tzinfo=None)
        c = croniter(cron_expr, now_local_naive)
        next_naive: datetime = c.get_next(datetime)
        next_dt = next_naive.replace(tzinfo=target_tz)
        return next_dt.astimezone(timezone.utc)

    if tt == "every":
        period = float(parsed.expr)  # type: ignore[arg-type]
        if prev_scheduled_run is not None:
            prev_p = ensure_datetime_utc(prev_scheduled_run)
            next_dt = prev_p + timedelta(seconds=period)
            if next_dt < now_ref:
                next_dt = now_ref + timedelta(seconds=period)
        else:
            # first run: wait one period (like Modal)
            next_dt = now_ref + timedelta(seconds=period)
        return next_dt

    if tt == "once":
        once_dt = ensure_datetime_utc(parsed.expr)  # type: ignore[arg-type]
        return max(once_dt, now_ref)

    raise InvalidTrigger(str(trigger), f"not a timed trigger (type={tt!r})")


def compute_run_interval(
    trigger: TTrigger,
    now: datetime,
    prev_interval_end: Optional[datetime],
    tz: str = "UTC",
) -> TTimeInterval:
    """Half-open `[start, end)` interval for a non-interval job run.

    `schedule:` and `every:` triggers carry continuity: `prev_interval_end`
    extends `start` backward to fill gaps (missed ticks, refresh cascade).
    All other trigger types return a point-in-time interval regardless of
    `prev_interval_end` — they model one-shot / event dispatches whose
    work-window has no meaningful "since last run" semantic.

    - `schedule:<cron>` → most recently ELAPSED cron interval:
      `[cron_prev(cron_floor(now)), cron_floor(now))`. `prev_interval_end`
      (if set) overrides `start`. In steady state those match.
    - `every:<period>` → `[prev_interval_end, now)` if set, else `[now - period, now)`.
    - `once:<datetime>` → always `[once, once)`.
    - `manual:` / `http:` / `webhook:` / `tag:` / `deployment:` /
      `job.success:` / `job.fail:` / `pipeline_name:` → always `[now, now)`.

    Args:
        trigger: Any normalized trigger string.
        now: Reference upper bound (typically the dispatch / `started_at` time).
        prev_interval_end: Last successful work-window end, or `None`.
            Only applies to `schedule:` and `every:` triggers.
        tz: IANA timezone for cron evaluation. Used only for `schedule:`.

    Returns:
        TTimeInterval: Half-open `[start, end)` tuple, both UTC stdlib `datetime`.

    Raises:
        InvalidTrigger: If `trigger` cannot be parsed.
    """
    now_p = ensure_datetime_utc(now)
    parsed = parse_trigger(trigger)
    tt = parsed.type

    if tt == "schedule":
        cron_expr = str(parsed.expr)
        target_tz = ZoneInfo(tz)
        now_local_naive = now_p.astimezone(target_tz).replace(tzinfo=None)
        # step forward 1us so an exact-on-tick `now` is treated as "already elapsed"
        # (matches cron_floor semantics); two get_prev calls give [prev_tick, floor)
        cron = croniter(cron_expr, now_local_naive + timedelta(microseconds=1))
        end_naive: datetime = cron.get_prev(datetime)
        start_naive: datetime = cron.get_prev(datetime)
        end_utc = end_naive.replace(tzinfo=target_tz).astimezone(timezone.utc)
        natural_start = start_naive.replace(tzinfo=target_tz).astimezone(timezone.utc)
        # prev_interval_end overrides cron-derived start for gap-filling
        start_utc = (
            ensure_datetime_utc(prev_interval_end)
            if prev_interval_end is not None
            else natural_start
        )
        return (start_utc, end_utc)

    if tt == "every":
        period = float(parsed.expr)  # type: ignore[arg-type]
        # continuity: prev_interval_end extends start backward; else [now-period, now)
        if prev_interval_end is not None:
            return (ensure_datetime_utc(prev_interval_end), now_p)
        return (now_p - timedelta(seconds=period), now_p)

    if tt == "once":
        # point-in-time: prev_interval_end ignored by design
        once_dt = ensure_datetime_utc(parsed.expr)  # type: ignore[arg-type]
        return (once_dt, once_dt)

    # all remaining trigger types: point-in-time at now (prev_interval_end ignored)
    return (now_p, now_p)


def resolve_interval_spec(spec: TIntervalSpec, cron_expr: str, tz: str = "UTC") -> TTimeInterval:
    """Resolve a TIntervalSpec into a concrete TTimeInterval in UTC.

    `start` is required and snapped backward to the latest cron tick <= start.
    `end` defaults to now, also snapped backward. Cron ticks are evaluated in
    `tz` so DST-sensitive expressions work correctly; the returned datetimes
    are always UTC.
    """
    target_tz = ZoneInfo(tz)
    raw_start = ensure_datetime_in_tz(spec["start"], target_tz)
    start = cron_floor(cron_expr, raw_start)

    end_str = spec.get("end")
    raw_end = ensure_datetime_in_tz(end_str, target_tz) if end_str else datetime.now(target_tz)
    end = cron_floor(cron_expr, raw_end)

    return start.astimezone(timezone.utc), end.astimezone(timezone.utc)


def cron_floor(cron_expr: str, dt: datetime) -> datetime:
    """Latest cron tick <= dt, preserving dt's timezone.

    Iterates cron in naive local time (stripping `dt`'s tzinfo internally) to
    get clean wall-clock semantics across DST transitions, then re-attaches
    `dt.tzinfo` to the result.
    """
    # get_prev returns the tick strictly before its base, so step forward by
    # one microsecond to include dt itself when it falls exactly on a tick
    # (seconds-epsilon is too coarse for sub-second inputs: `11:59:59.999999`
    # would yield `12:00:00` which is > dt).
    base = (dt.replace(tzinfo=None) if dt.tzinfo else dt) + timedelta(microseconds=1)
    cron = croniter(cron_expr, base)
    prev_naive: datetime = cron.get_prev(datetime)
    if dt.tzinfo is not None:
        return prev_naive.replace(tzinfo=dt.tzinfo)
    return prev_naive


def is_cron_expression(s: str) -> bool:
    """Check if string is a valid cron expression."""
    return croniter.is_valid(s)


def _check_schedule_run_freshness(
    upstream_ref: str,
    cron_expr: str,
    now_utc: datetime,
    prev_interval_end: datetime,
    tz: str = "UTC",
) -> Tuple[bool, str]:
    """Checks if last (per `utc_now`) cron interval was processed by `upstream_ref`"""
    # the just-elapsed cron tick in target tz, converted back to UTC
    expected = cron_floor(cron_expr, now_utc.astimezone(ZoneInfo(tz))).astimezone(timezone.utc)
    if ensure_datetime_utc(prev_interval_end) < expected:
        return False, f"upstream {upstream_ref} missing run for {expected}"
    return True, ""


def _check_every_freshness(
    upstream_ref: str,
    period_seconds: float,
    now_utc: datetime,
    prev_interval_end: datetime,
) -> Tuple[bool, str]:
    """every: upstream's last-completed `interval_end` is within the previous period"""
    # interval_end is exclusive: elapsed == period means `now` has crossed into the next period
    # and the previous one is unprocessed — stale
    elapsed = (now_utc - ensure_datetime_utc(prev_interval_end)).total_seconds()
    if elapsed >= period_seconds:
        return (
            False,
            (
                f"upstream {upstream_ref} has a missing run"
                f" (last {elapsed:.0f}s ago, period {period_seconds:.0f}s)"
            ),
        )
    return True, ""


def _resolve_upstream(
    constraint: TFreshnessConstraint,
    all_jobs: Dict[str, TJobDefinition],
) -> Tuple[str, str, TJobDefinition, Optional[str]]:
    """Parse constraint and resolve upstream job.

    Returns (freshness_type, upstream_ref, upstream_job, trigger_type).
    trigger_type is None when default_trigger is absent.
    """
    parsed_constraint = parse_freshness_constraint(constraint)
    upstream_ref = parsed_constraint.expr
    freshness_type = parsed_constraint.type
    upstream_job = all_jobs[upstream_ref]

    default_trigger = upstream_job.get("default_trigger")
    trigger_type = None
    if default_trigger:
        parsed = parse_trigger(default_trigger)
        trigger_type = parsed.type

    return freshness_type, upstream_ref, upstream_job, trigger_type


def check_all_upstream_run_fresh(
    freshness_constraints: List[TFreshnessConstraint],
    all_jobs: Dict[str, TJobDefinition],
    prev_interval_ends: Dict[str, Optional[datetime]],
    now_utc: Optional[datetime] = None,
) -> Tuple[bool, List[str]]:
    """Check run-based freshness constraints against each upstream's last-completed
    `interval_end`.

    `prev_interval_ends[upstream_ref]` is the `interval_end` of the upstream's last
    successful run (`None` if it has never completed or its freshness state was
    reset). Dispatches per-upstream based on the upstream's `default_trigger` type:

    - `schedule:` without `interval` → `interval_end` covers the most recently
      elapsed cron tick
    - `every:` → `interval_end` is within the previous period
    - event/manual → `interval_end` is set (any completion is enough)
    """
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    reasons: List[str] = []

    for constraint in freshness_constraints:
        freshness_type, upstream_ref, upstream_job, trigger_type = _resolve_upstream(
            constraint, all_jobs
        )

        if upstream_job["entry_point"]["job_type"] == "interactive":
            reasons.append(f"upstream {upstream_ref} is an interactive job (cannot be fresh)")
            continue

        if freshness_type == "job.is_matching_interval_fresh":
            reasons.append(
                f"upstream {upstream_ref} —"
                " job.is_matching_interval_fresh requires interval-based downstream"
            )
            continue

        prev_interval_end = prev_interval_ends.get(upstream_ref)
        if prev_interval_end is None:
            reasons.append(
                f"upstream {upstream_ref} didn't process any intervals yet (refresh pending)"
            )
            continue

        if trigger_type == "schedule":
            us_cron = maybe_parse_schedule(upstream_job)
            us_tz = upstream_job.get("require", {}).get("timezone", "UTC")
            fresh, reason = _check_schedule_run_freshness(
                upstream_ref, us_cron, now_utc, prev_interval_end, tz=us_tz
            )
        elif trigger_type == "every":
            parsed = parse_trigger(upstream_job["default_trigger"])
            period = float(parsed.expr)  # type: ignore[arg-type]
            fresh, reason = _check_every_freshness(upstream_ref, period, now_utc, prev_interval_end)
        else:
            # event / manual — any completed `interval_end` is sufficient
            continue

        if not fresh:
            reasons.append(reason)

    return len(reasons) == 0, reasons
