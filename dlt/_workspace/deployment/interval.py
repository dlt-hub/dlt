"""Interval computation and upstream freshness checks."""

from datetime import datetime, timedelta, timezone  # noqa: I251
from typing import Dict, Iterator, List, NamedTuple, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

from dlt import version
from dlt.common.exceptions import MissingDependencyException
from dlt.common.time import ensure_pendulum_datetime_non_utc, ensure_pendulum_datetime_utc
from dlt.common.typing import TAnyDateTime, TTimeInterval as TInterval

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


def _stdlib_utc(value: TAnyDateTime) -> datetime:
    """Coerce to a plain stdlib `datetime` in UTC (`tzinfo=timezone.utc`).

    Thin wrapper over `ensure_pendulum_datetime_utc` that strips pendulum's
    subclass so callers only handle stdlib datetimes.
    """
    pdt = ensure_pendulum_datetime_utc(value)
    return datetime(
        pdt.year,
        pdt.month,
        pdt.day,
        pdt.hour,
        pdt.minute,
        pdt.second,
        pdt.microsecond,
        tzinfo=timezone.utc,
    )


def _stdlib_in_tz(value: TAnyDateTime, tz: ZoneInfo) -> datetime:
    """Coerce to stdlib `datetime` in target tz. Naive inputs are attached to `tz`.

    Matches the pre-existing dlt convention: a naive user-supplied string means
    "local wall clock in the job's timezone", not UTC.
    """
    pdt = ensure_pendulum_datetime_non_utc(value)
    stdlib_dt = datetime(
        pdt.year,
        pdt.month,
        pdt.day,
        pdt.hour,
        pdt.minute,
        pdt.second,
        pdt.microsecond,
        tzinfo=pdt.tzinfo,
    )
    if stdlib_dt.tzinfo is None:
        return stdlib_dt.replace(tzinfo=tz)
    return stdlib_dt.astimezone(tz)


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
    now_ref = _stdlib_utc(now_reference)

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
            prev_p = _stdlib_utc(prev_scheduled_run)
            next_dt = prev_p + timedelta(seconds=period)
            if next_dt < now_ref:
                next_dt = now_ref + timedelta(seconds=period)
        else:
            # first run: wait one period (like Modal)
            next_dt = now_ref + timedelta(seconds=period)
        return next_dt

    if tt == "once":
        once_dt = _stdlib_utc(parsed.expr)  # type: ignore[arg-type]
        return max(once_dt, now_ref)

    raise InvalidTrigger(str(trigger), f"not a timed trigger (type={tt!r})")


def compute_run_interval(
    trigger: TTrigger,
    now: datetime,
    prev_interval_end: Optional[datetime],
    tz: str = "UTC",
) -> TInterval:
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
        TInterval: Half-open `[start, end)` tuple, both UTC stdlib `datetime`.

    Raises:
        InvalidTrigger: If `trigger` cannot be parsed.
    """
    now_p = _stdlib_utc(now)
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
            _stdlib_utc(prev_interval_end) if prev_interval_end is not None else natural_start
        )
        return (start_utc, end_utc)

    if tt == "every":
        period = float(parsed.expr)  # type: ignore[arg-type]
        # continuity: prev_interval_end extends start backward; else [now-period, now)
        if prev_interval_end is not None:
            return (_stdlib_utc(prev_interval_end), now_p)
        return (now_p - timedelta(seconds=period), now_p)

    if tt == "once":
        # point-in-time: prev_interval_end ignored by design
        once_dt = _stdlib_utc(parsed.expr)  # type: ignore[arg-type]
        return (once_dt, once_dt)

    # all remaining trigger types: point-in-time at now (prev_interval_end ignored)
    return (now_p, now_p)


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
    """Resolve a TIntervalSpec into a concrete TInterval in UTC.

    `start` is required and snapped backward to the latest cron tick <= start.
    `end` defaults to now, also snapped backward. Cron ticks are evaluated in
    `tz` so DST-sensitive expressions work correctly; the returned datetimes
    are always UTC.
    """
    target_tz = ZoneInfo(tz)
    raw_start = _stdlib_in_tz(spec["start"], target_tz)
    start = cron_floor(cron_expr, raw_start)

    end_str = spec.get("end")
    raw_end = _stdlib_in_tz(end_str, target_tz) if end_str else datetime.now(target_tz)
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


def iter_intervals(
    cron_expr: str,
    overall: TInterval,
    tz: str = "UTC",
) -> Iterator[TInterval]:
    """Yield discrete [tick_n, tick_n+1) intervals from cron within overall range.

    Cron is evaluated in `tz` (so DST-sensitive expressions tick on local walls),
    but yielded datetimes are always UTC. `overall` may be in any timezone;
    it is converted to `tz` for iteration.
    """
    start, end = overall
    if start >= end:
        return

    target_tz = ZoneInfo(tz)
    start_tz = _stdlib_utc(start).astimezone(target_tz)
    end_tz = _stdlib_utc(end).astimezone(target_tz)

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
    overall: TInterval,
    completed: Sequence[TInterval],
    tz: str = "UTC",
) -> List[TInterval]:
    """Return all incomplete intervals within overall, earliest first.

    Args:
        cron_expr: Cron expression defining interval boundaries.
        overall: The full time range to consider.
        completed: Sorted, coalesced list of completed intervals.
        tz: IANA timezone for cron evaluation. Yielded intervals are UTC.
    """
    effective_start, comp_idx = _trim_leading_completed(overall[0], completed)
    result: List[TInterval] = []
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
    overall: TInterval,
    completed: Sequence[TInterval],
    tz: str = "UTC",
) -> Optional[TInterval]:
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
    start: datetime, completed: Sequence[TInterval]
) -> Tuple[datetime, int]:
    """Advance start past the first completed range if it covers the beginning."""
    if completed and completed[0][0] <= start:
        return completed[0][1], 1
    return start, 0


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
    if _stdlib_utc(prev_interval_end) < expected:
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
    elapsed = (now_utc - _stdlib_utc(prev_interval_end)).total_seconds()
    if elapsed >= period_seconds:
        return (
            False,
            (
                f"upstream {upstream_ref} has a missing run"
                f" (last {elapsed:.0f}s ago, period {period_seconds:.0f}s)"
            ),
        )
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
