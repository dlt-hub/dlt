"""Interval-store and matching-intervals freshness checks."""

from datetime import datetime, timedelta, timezone  # noqa: I251
from typing import Dict, Iterator, List, NamedTuple, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

from dlt import version
from dlt.common.exceptions import MissingDependencyException
from dlt.common.time import ensure_datetime_utc
from dlt.common.typing import TTimeInterval

from dlt._workspace.deployment._trigger_helpers import maybe_parse_schedule
from dlt._workspace.deployment.interval import (
    _resolve_upstream,
    resolve_interval_spec,
)
from dlt._workspace.deployment.typing import (
    TFreshnessConstraint,
    TJobDefinition,
)

try:
    from croniter import croniter
except ModuleNotFoundError:
    raise MissingDependencyException(
        "dltHub workspace",
        [f"{version.DLT_PKG_NAME}[workspace]"],
    )


def sort_and_coalesce(intervals: Sequence[TTimeInterval]) -> List[TTimeInterval]:
    """Sort intervals by start and merge adjacent/overlapping into contiguous ranges."""
    if not intervals:
        return []
    sorted_ivs = sorted(intervals, key=lambda iv: iv[0])
    merged: List[TTimeInterval] = [sorted_ivs[0]]
    for iv in sorted_ivs[1:]:
        prev_start, prev_end = merged[-1]
        if iv[0] <= prev_end:
            merged[-1] = (prev_start, max(prev_end, iv[1]))
        else:
            merged.append(iv)
    return merged


def iter_intervals(
    cron_expr: str,
    overall: TTimeInterval,
    tz: str = "UTC",
) -> Iterator[TTimeInterval]:
    """Yield discrete [tick_n, tick_n+1) intervals from cron within overall range.

    Cron is evaluated in `tz` (so DST-sensitive expressions tick on local walls),
    but yielded datetimes are always UTC. `overall` may be in any timezone;
    it is converted to `tz` for iteration.
    """
    start, end = overall
    if start >= end:
        return

    target_tz = ZoneInfo(tz)
    start_tz = ensure_datetime_utc(start).astimezone(target_tz)
    end_tz = ensure_datetime_utc(end).astimezone(target_tz)

    # iterate cron in naive local time to get clean wall-clock semantics across
    # DST transitions (see next_scheduled_run for rationale)
    start_naive = start_tz.replace(tzinfo=None) - timedelta(seconds=1)
    end_naive = end_tz.replace(tzinfo=None)
    cron = croniter(cron_expr, start_naive)
    tick_naive: datetime = cron.get_next(datetime)

    while tick_naive < end_naive:
        next_naive: datetime = cron.get_next(datetime)
        if next_naive <= end_naive:
            tick_utc = tick_naive.replace(tzinfo=target_tz).astimezone(timezone.utc)
            next_utc = next_naive.replace(tzinfo=target_tz).astimezone(timezone.utc)
            yield (tick_utc, next_utc)
        tick_naive = next_naive


def get_eligible_intervals(
    cron_expr: str,
    overall: TTimeInterval,
    completed: Sequence[TTimeInterval],
    tz: str = "UTC",
) -> List[TTimeInterval]:
    """Return all incomplete intervals within overall, earliest first.

    Args:
        cron_expr: Cron expression defining interval boundaries.
        overall: The full time range to consider.
        completed: Sorted, coalesced list of completed intervals.
        tz: IANA timezone for cron evaluation. Yielded intervals are UTC.
    """
    effective_start, comp_idx = _trim_leading_completed(overall[0], completed)
    result: List[TTimeInterval] = []
    for iv in iter_intervals(cron_expr, (effective_start, overall[1]), tz=tz):
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
    overall: TTimeInterval,
    completed: Sequence[TTimeInterval],
    tz: str = "UTC",
) -> Optional[TTimeInterval]:
    """First incomplete interval, or None if all done.

    Args:
        cron_expr: Cron expression defining interval boundaries.
        overall: The full time range to consider.
        completed: Sorted, coalesced list of completed intervals.
        tz: IANA timezone for cron evaluation. Returned interval is UTC.
    """
    effective_start, comp_idx = _trim_leading_completed(overall[0], completed)
    for interval in iter_intervals(cron_expr, (effective_start, overall[1]), tz=tz):
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
    start: datetime, completed: Sequence[TTimeInterval]
) -> Tuple[datetime, int]:
    """Advance start past the first completed range if it covers the beginning."""
    if completed and completed[0][0] <= start:
        return completed[0][1], 1
    return start, 0


class TIntervalFreshnessCheck(NamedTuple):
    """A resolved interval completion query for freshness checking."""

    upstream_ref: str
    effective_interval: TTimeInterval
    """The interval to check completion for."""
    reason_if_not_completed: str
    """Reason string to use if the interval is not completed."""


def resolve_interval_freshness_checks(
    downstream_interval: TTimeInterval,
    downstream_overall: TTimeInterval,
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
    completions: Dict[Tuple[str, TTimeInterval], bool],
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
