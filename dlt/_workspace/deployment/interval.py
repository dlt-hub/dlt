"""Interval computation and upstream freshness checks."""

from datetime import datetime  # noqa: I251
from typing import Dict, Iterator, List, NamedTuple, Optional, Sequence, Tuple

from dlt import version
from dlt.common.exceptions import MissingDependencyException
from dlt.common.pendulum import pendulum
from dlt.common.time import ensure_pendulum_datetime_non_utc, ensure_pendulum_datetime_utc
from dlt.common.typing import TTimeInterval as TInterval

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


TLastRunInfo = Optional[Tuple[bool, datetime]]
"""Last run status for freshness: `None` = no runs, `(is_completed, scheduled_at)` otherwise."""


class TNextScheduledRun(NamedTuple):
    scheduled_at: datetime
    """Timezone-aware datetime when the job should run."""
    interval: Optional[TInterval]
    """Interval the run covers, or None for once: triggers."""


def next_scheduled_run(
    trigger: TTrigger,
    now_reference: datetime,
    tz: str = "UTC",
    prev_scheduled_run: Optional[datetime] = None,
) -> TNextScheduledRun:
    """Compute the next scheduled run for a timed trigger.

    All returned datetimes are UTC.

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
    now_ref: pendulum.DateTime = pendulum.instance(
        ensure_pendulum_datetime_utc(now_reference), tz=pendulum.UTC
    )

    if tt == "schedule":
        cron_expr = str(parsed.expr)
        target_tz = pendulum.timezone(tz)
        now_tz = now_ref.in_timezone(target_tz)
        c = croniter(cron_expr, now_tz)
        next_dt = pendulum.instance(c.get_next(pendulum.DateTime), tz=target_tz)
        prev_tick = pendulum.instance(cron_floor(cron_expr, now_tz), tz=target_tz)
        return TNextScheduledRun(
            scheduled_at=next_dt.in_timezone(pendulum.UTC),
            interval=(
                prev_tick.in_timezone(pendulum.UTC),
                next_dt.in_timezone(pendulum.UTC),
            ),
        )

    if tt == "every":
        period = float(parsed.expr)  # type: ignore[arg-type]
        if prev_scheduled_run is not None:
            prev_p: pendulum.DateTime = pendulum.instance(
                ensure_pendulum_datetime_utc(prev_scheduled_run), tz=pendulum.UTC
            )
            next_dt = prev_p.add(seconds=period)
            if next_dt < now_ref:
                next_dt = now_ref.add(seconds=period)
        else:
            # first run: wait one period (like Modal)
            next_dt = now_ref.add(seconds=period)
        interval_end = next_dt.add(seconds=period)
        return TNextScheduledRun(scheduled_at=next_dt, interval=(next_dt, interval_end))

    if tt == "once":
        once_dt: pendulum.DateTime = pendulum.instance(
            ensure_pendulum_datetime_utc(parsed.expr), tz=pendulum.UTC  # type: ignore[arg-type]
        )
        scheduled = max(once_dt, now_ref)
        return TNextScheduledRun(scheduled_at=scheduled, interval=None)

    raise InvalidTrigger(str(trigger), f"not a timed trigger (type={tt!r})")


def sort_and_coalesce(intervals: Sequence[TInterval]) -> List[TInterval]:
    """Sort intervals by start and merge adjacent/overlapping into contiguous ranges."""
    if not intervals:
        return []
    sorted_ivs = sorted(intervals, key=lambda iv: iv[0])
    merged: List[TInterval] = [sorted_ivs[0]]
    for iv in sorted_ivs[1:]:
        prev_start, prev_end = merged[-1]
        if iv[0] <= prev_end:
            merged[-1] = (prev_start, max(prev_end, iv[1]))
        else:
            merged.append(iv)
    return merged


def resolve_interval_spec(spec: TIntervalSpec, cron_expr: str, tz: str = "UTC") -> TInterval:
    """Resolve a TIntervalSpec into a concrete TInterval.

    `start` is required and snapped backward to the latest cron tick <= start.
    `end` defaults to now, also snapped backward. All ticks are in `tz`.
    """
    target_tz = pendulum.timezone(tz)
    raw_start = ensure_pendulum_datetime_non_utc(spec["start"]).in_timezone(target_tz)
    start = cron_floor(cron_expr, raw_start)

    end_str = spec.get("end")
    raw_end = (
        ensure_pendulum_datetime_non_utc(end_str).in_timezone(target_tz)
        if end_str
        else pendulum.now(tz)
    )
    end = cron_floor(cron_expr, raw_end)

    return start, end


def cron_floor(cron_expr: str, dt: datetime) -> datetime:
    """Latest cron tick <= dt, preserving dt's timezone."""
    # get_prev returns the tick strictly before dt, so step forward 1s
    # to include dt itself when it falls exactly on a tick
    cron = croniter(cron_expr, dt + pendulum.duration(seconds=1))
    return pendulum.instance(cron.get_prev(pendulum.DateTime), tz=dt.tzinfo)


def is_cron_expression(s: str) -> bool:
    """Check if string is a valid cron expression."""
    return croniter.is_valid(s)


def iter_intervals(
    cron_expr: str,
    overall: TInterval,
) -> Iterator[TInterval]:
    """Yield discrete [tick_n, tick_n+1) intervals from cron within overall range."""
    start, end = overall
    if start >= end:
        return

    tz = start.tzinfo
    cron = croniter(cron_expr, start - pendulum.duration(seconds=1))
    tick = pendulum.instance(cron.get_next(pendulum.DateTime), tz=tz)

    while tick < end:
        next_tick = pendulum.instance(cron.get_next(pendulum.DateTime), tz=tz)
        if next_tick <= end:
            yield (tick, next_tick)
        tick = next_tick


def get_eligible_intervals(
    cron_expr: str,
    overall: TInterval,
    completed: Sequence[TInterval],
) -> List[TInterval]:
    """Return all incomplete intervals within overall, earliest first.

    Args:
        cron_expr: Cron expression defining interval boundaries.
        overall: The full time range to consider.
        completed: Sorted, coalesced list of completed intervals.
    """
    effective_start, comp_idx = _trim_leading_completed(overall[0], completed)
    result: List[TInterval] = []
    for iv in iter_intervals(cron_expr, (effective_start, overall[1])):
        while comp_idx < len(completed) and completed[comp_idx][1] <= iv[0]:
            comp_idx += 1
        if (
            comp_idx < len(completed)
            and completed[comp_idx][0] <= iv[0]
            and completed[comp_idx][1] >= iv[1]
        ):
            continue
        result.append(iv)
    return result


def next_eligible_interval(
    cron_expr: str,
    overall: TInterval,
    completed: Sequence[TInterval],
) -> Optional[TInterval]:
    """First incomplete interval, or None if all done.

    Args:
        cron_expr: Cron expression defining interval boundaries.
        overall: The full time range to consider.
        completed: Sorted, coalesced list of completed intervals.
    """
    effective_start, comp_idx = _trim_leading_completed(overall[0], completed)
    for interval in iter_intervals(cron_expr, (effective_start, overall[1])):
        # advance past completed ranges that end before this interval
        while comp_idx < len(completed) and completed[comp_idx][1] <= interval[0]:
            comp_idx += 1
        # skip if current completed range fully covers this interval
        if (
            comp_idx < len(completed)
            and completed[comp_idx][0] <= interval[0]
            and completed[comp_idx][1] >= interval[1]
        ):
            continue
        return interval
    return None


def _trim_leading_completed(
    start: datetime, completed: Sequence[TInterval]
) -> Tuple[datetime, int]:
    """Advance start past the first completed range if it covers the beginning."""
    if completed and completed[0][0] <= start:
        return completed[0][1], 1
    return start, 0


def _check_schedule_run_freshness(
    upstream_ref: str,
    cron_expr: str,
    last_run: TLastRunInfo,
    tz: str = "UTC",
) -> Tuple[bool, str]:
    """schedule: upstream WITHOUT interval — last run completed at most recent cron tick."""
    if last_run is None:
        return False, f"upstream {upstream_ref} has no completed runs"
    is_completed, scheduled_at = last_run
    if not is_completed:
        return False, f"upstream {upstream_ref} last run not completed"
    expected = cron_floor(cron_expr, pendulum.now(tz))
    if scheduled_at < expected:
        return False, f"upstream {upstream_ref} missing run for {expected}"
    return True, ""


def _check_every_freshness(
    upstream_ref: str,
    period_seconds: float,
    last_run: TLastRunInfo,
) -> Tuple[bool, str]:
    """every: upstream — last run completed AND within period."""
    if last_run is None:
        return False, f"upstream {upstream_ref} has no completed runs"
    is_completed, scheduled_at = last_run
    if not is_completed:
        return False, f"upstream {upstream_ref} last run not completed"
    elapsed = (pendulum.now("UTC") - ensure_pendulum_datetime_utc(scheduled_at)).total_seconds()
    if elapsed >= period_seconds:
        return (
            False,
            (
                f"upstream {upstream_ref} has a missing run"
                f" (last {elapsed:.0f}s ago, period {period_seconds:.0f}s)"
            ),
        )
    return True, ""


def _check_event_freshness(
    upstream_ref: str,
    last_run: TLastRunInfo,
) -> Tuple[bool, str]:
    """event/manual upstream — last run completed."""
    if last_run is None:
        return False, f"upstream {upstream_ref} has no completed runs"
    is_completed, _ = last_run
    if not is_completed:
        return False, f"upstream {upstream_ref} last run not completed"
    return True, ""


class TIntervalFreshnessCheck(NamedTuple):
    """A resolved interval completion query for freshness checking."""

    upstream_ref: str
    effective_interval: TInterval
    """The interval to check completion for."""
    reason_if_not_completed: str
    """Reason string to use if the interval is not completed."""


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


def resolve_interval_freshness_checks(
    downstream_interval: TInterval,
    downstream_overall: TInterval,
    freshness_constraints: List[TFreshnessConstraint],
    all_jobs: Dict[str, TJobDefinition],
) -> Tuple[List[TIntervalFreshnessCheck], List[str]]:
    """Resolve interval freshness constraints into completion queries.

    Returns:
        checks: Completion queries to run against the interval store.
        immediate_reasons: Errors that don't need a store lookup.
    """
    checks: List[TIntervalFreshnessCheck] = []
    reasons: List[str] = []

    for constraint in freshness_constraints:
        freshness_type, upstream_ref, upstream_job, trigger_type = _resolve_upstream(
            constraint, all_jobs
        )

        if upstream_job["entry_point"]["job_type"] == "interactive":
            reasons.append(f"upstream {upstream_ref} is an interactive job (cannot be fresh)")
            continue

        has_interval = "interval" in upstream_job
        if not (trigger_type == "schedule" and has_interval):
            reasons.append(
                f"upstream {upstream_ref} has no interval —"
                " interval freshness requires schedule with interval"
            )
            continue

        upstream_spec = upstream_job["interval"]
        us_cron = maybe_parse_schedule(upstream_job)
        us_tz = upstream_job.get("require", {}).get("timezone", "UTC")
        upstream_overall = resolve_interval_spec(upstream_spec, us_cron, tz=us_tz)
        upstream_start, upstream_end = upstream_overall

        if freshness_type == "job.is_matching_interval_fresh":
            # non-overlapping → auto-pass
            if downstream_interval[1] <= upstream_start:
                continue
            effective_start = max(downstream_interval[0], upstream_start)
            effective_end = min(downstream_interval[1], upstream_end)
            if effective_start >= effective_end:
                reasons.append(
                    f"upstream {upstream_ref} has no elapsed intervals covering"
                    f" [{downstream_interval[0]}, {downstream_interval[1]})"
                )
                continue
            checks.append(
                TIntervalFreshnessCheck(
                    upstream_ref=upstream_ref,
                    effective_interval=(effective_start, effective_end),
                    reason_if_not_completed=(
                        f"upstream {upstream_ref} not fresh"
                        f" for [{effective_start}, {effective_end})"
                    ),
                )
            )

        elif freshness_type == "job.is_fresh":
            ds_start, ds_end = downstream_overall
            # non-overlapping → auto-pass
            if ds_end <= upstream_start:
                continue
            effective_start = max(ds_start, upstream_start)
            effective_end = min(ds_end, upstream_end)
            if effective_start >= effective_end:
                reasons.append(f"upstream {upstream_ref} has no elapsed intervals in shared range")
                continue
            checks.append(
                TIntervalFreshnessCheck(
                    upstream_ref=upstream_ref,
                    effective_interval=(effective_start, effective_end),
                    reason_if_not_completed=(
                        f"upstream {upstream_ref} not fully fresh for overall interval"
                    ),
                )
            )

    return checks, reasons


def check_all_upstream_interval_fresh(
    checks: List[TIntervalFreshnessCheck],
    completions: Dict[Tuple[str, TInterval], bool],
    immediate_reasons: Optional[List[str]] = None,
) -> Tuple[bool, List[str]]:
    """Evaluate interval freshness given pre-fetched completion data.

    Args:
        checks: From `resolve_interval_freshness_checks`.
        completions: Maps `(upstream_ref, effective_interval)` to `is_completed`.
        immediate_reasons: Pre-computed error reasons (passed through).
    """
    reasons: List[str] = list(immediate_reasons or [])
    for check in checks:
        key = (check.upstream_ref, check.effective_interval)
        if not completions.get(key, False):
            reasons.append(check.reason_if_not_completed)
    return len(reasons) == 0, reasons


def resolve_run_freshness_refs(
    freshness_constraints: List[TFreshnessConstraint],
    all_jobs: Dict[str, TJobDefinition],
) -> Tuple[List[str], List[str]]:
    """Resolve which upstream refs need last-run data for run-based freshness.

    Returns (refs_to_query, immediate_reasons). immediate_reasons are
    errors that don't need a run lookup (interactive upstream, wrong
    constraint type).
    """
    refs: List[str] = []
    reasons: List[str] = []

    for constraint in freshness_constraints:
        freshness_type, upstream_ref, upstream_job, trigger_type = _resolve_upstream(
            constraint, all_jobs
        )
        if upstream_job["entry_point"]["job_type"] == "interactive":
            reasons.append(f"upstream {upstream_ref} is an interactive job (cannot be fresh)")
        elif freshness_type == "job.is_matching_interval_fresh":
            reasons.append(
                f"upstream {upstream_ref} —"
                " job.is_matching_interval_fresh requires interval-based downstream"
            )
        else:
            if upstream_ref not in refs:
                refs.append(upstream_ref)

    return refs, reasons


def check_all_upstream_run_fresh(
    freshness_constraints: List[TFreshnessConstraint],
    all_jobs: Dict[str, TJobDefinition],
    last_runs: Dict[str, TLastRunInfo],
) -> Tuple[bool, List[str]]:
    """Check run-based freshness constraints given pre-fetched last-run info.

    Dispatches per-upstream based on the upstream's `default_trigger` type:
    - `schedule:` without `interval` → last completed at current cron tick
    - `every:` → last completed within period
    - event/manual → last run completed
    """
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

        run_info = last_runs.get(upstream_ref)

        if trigger_type == "schedule":
            us_cron = maybe_parse_schedule(upstream_job)
            us_tz = upstream_job.get("require", {}).get("timezone", "UTC")
            fresh, reason = _check_schedule_run_freshness(upstream_ref, us_cron, run_info, tz=us_tz)
        elif trigger_type == "every":
            parsed = parse_trigger(upstream_job["default_trigger"])
            period = float(parsed.expr)  # type: ignore[arg-type]
            fresh, reason = _check_every_freshness(upstream_ref, period, run_info)
        else:
            fresh, reason = _check_event_freshness(upstream_ref, run_info)

        if not fresh:
            reasons.append(reason)

    return len(reasons) == 0, reasons
