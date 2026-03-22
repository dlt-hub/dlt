"""Interval computation, tracking, and upstream freshness checks.

Implements interval scheduling: cron + overall range → discrete
half-open intervals → track completed → check upstream freshness before
processing downstream intervals.
"""

from typing import Dict, Iterator, List, Optional, Tuple

from dlt.common.pendulum import pendulum
from dlt.common.time import ensure_pendulum_datetime_utc

from dlt._workspace.deployment._trigger_helpers import (
    maybe_parse_schedule,
    parse_freshness_constraint,
    parse_trigger,
)
from dlt._workspace.deployment.typing import (
    TFreshnessConstraint,
    TIntervalSpec,
    TJobDefinition,
    TTrigger,
)

TInterval = Tuple[pendulum.DateTime, pendulum.DateTime]
"""Half-open interval [start, end)."""


def resolve_interval_spec(spec: TIntervalSpec, cron_expr: str) -> TInterval:
    """Resolve a TIntervalSpec into a concrete TInterval.

    `start` defaults to now if omitted. `end` defaults to the end of the
    last fully elapsed cron interval (i.e. the latest cron tick <= now).
    """
    from croniter import croniter

    start_str = spec.get("start")
    end_str = spec.get("end")
    start = ensure_pendulum_datetime_utc(start_str) if start_str else pendulum.now("UTC")

    if end_str:
        end = ensure_pendulum_datetime_utc(end_str)
    else:
        now = pendulum.now("UTC")
        cron = croniter(cron_expr, now)
        end = pendulum.instance(cron.get_prev(pendulum.DateTime), tz="UTC")

    return start, end


def iter_intervals(
    cron_expr: str,
    overall: TInterval,
) -> Iterator[TInterval]:
    """Yield discrete [tick_n, tick_n+1) intervals from cron within overall range."""
    from croniter import croniter

    start, end = overall
    if start >= end:
        return

    cron = croniter(cron_expr, start - pendulum.duration(seconds=1))
    tick = pendulum.instance(cron.get_next(pendulum.DateTime), tz="UTC")

    while tick < end:
        next_tick = pendulum.instance(cron.get_next(pendulum.DateTime), tz="UTC")
        if next_tick <= end:
            yield (tick, next_tick)
        tick = next_tick


def compute_intervals(cron_expr: str, overall: TInterval) -> List[TInterval]:
    """All discrete intervals from cron within overall range."""
    return list(iter_intervals(cron_expr, overall))


class IntervalStore:
    """Tracks completed job intervals using in-memory DuckDB."""

    def __init__(self) -> None:
        import duckdb

        self._conn = duckdb.connect(":memory:")
        self._conn.execute(
            "CREATE TABLE completed_intervals ("
            "  job_ref VARCHAR,"
            "  start_ts TIMESTAMPTZ,"
            "  end_ts TIMESTAMPTZ"
            ")"
        )

    def mark_completed(
        self, job_ref: str, start: pendulum.DateTime, end: pendulum.DateTime
    ) -> None:
        """Record an interval as completed."""
        self._conn.execute(
            "INSERT INTO completed_intervals VALUES (?, ?, ?)",
            [job_ref, start, end],
        )

    def is_range_covered(
        self, job_ref: str, start: pendulum.DateTime, end: pendulum.DateTime
    ) -> bool:
        """Check if [start, end) is fully covered by completed intervals."""
        rows = self._conn.execute(
            "SELECT start_ts, end_ts FROM completed_intervals "
            "WHERE job_ref = ? AND end_ts > ? AND start_ts < ? "
            "ORDER BY start_ts",
            [job_ref, start, end],
        ).fetchall()
        if not rows:
            return False
        if rows[0][0] > start:
            return False
        merged_end = rows[0][1]
        for row in rows[1:]:
            if row[0] > merged_end:
                return False
            merged_end = max(merged_end, row[1])
        return bool(merged_end >= end)

    def get_completed_ranges(self, job_ref: str) -> List[TInterval]:
        """All completed intervals for a job, ordered by start."""
        rows = self._conn.execute(
            "SELECT start_ts, end_ts FROM completed_intervals WHERE job_ref = ? ORDER BY start_ts",
            [job_ref],
        ).fetchall()
        return [
            (pendulum.instance(r[0], tz="UTC"), pendulum.instance(r[1], tz="UTC")) for r in rows
        ]

    def close(self) -> None:
        self._conn.close()


def get_eligible_intervals(
    job_ref: str,
    cron_expr: str,
    overall: TInterval,
    store: IntervalStore,
) -> List[TInterval]:
    """Return intervals that are due but not completed, earliest first."""
    return [
        iv
        for iv in iter_intervals(cron_expr, overall)
        if not store.is_range_covered(job_ref, iv[0], iv[1])
    ]


def next_eligible_interval(
    job_ref: str,
    cron_expr: str,
    overall: TInterval,
    store: IntervalStore,
) -> Optional[TInterval]:
    """First incomplete interval, or None if all done."""
    for iv in iter_intervals(cron_expr, overall):
        if not store.is_range_covered(job_ref, iv[0], iv[1]):
            return iv
    return None


def check_upstream_freshness(
    downstream_interval: TInterval,
    downstream_overall: TInterval,
    upstream_ref: str,
    upstream_overall: TInterval,
    trigger_type: str,
    store: IntervalStore,
) -> Tuple[bool, str]:
    """Check if a single upstream freshness condition is met.

    Two constraint types:

    `job.is_matching_interval_fresh` — the downstream's specific interval
    must be fully covered by completed upstream intervals. The downstream
    interval is clipped to the upstream's overall range. If no overlap
    (downstream interval falls before upstream start), the constraint is
    satisfied — we don't block on data that will never exist.

    `job.is_fresh` — the upstream's entire overall interval (intersected
    with the downstream's overall interval) must be complete. This is
    stricter: the downstream won't run ANY interval until the upstream
    is fully caught up over the shared range.

    Args:
        downstream_interval: The specific `[start, end)` being processed.
        downstream_overall: The downstream job's resolved overall interval.
        upstream_ref: The upstream job_ref.
        upstream_overall: The upstream job's resolved overall interval.
        trigger_type: `"job.is_matching_interval_fresh"` or `"job.is_fresh"`.
        store: Interval completion store.

    Returns:
        Tuple of `(is_fresh, reason)`. `reason` is empty when fresh.
    """
    us_start, us_end = upstream_overall

    if trigger_type == "job.is_matching_interval_fresh":
        # clip downstream interval to upstream's overall range
        effective_start = max(downstream_interval[0], us_start)
        effective_end = min(downstream_interval[1], us_end)
        if effective_start >= effective_end:
            return True, ""
        if store.is_range_covered(upstream_ref, effective_start, effective_end):
            return True, ""
        return False, f"upstream {upstream_ref} not fresh for [{effective_start}, {effective_end})"

    if trigger_type == "job.is_fresh":
        # intersection of overall intervals
        ds_start, ds_end = downstream_overall
        effective_start = max(ds_start, us_start)
        effective_end = min(ds_end, us_end)
        if effective_start >= effective_end:
            return True, ""
        if store.is_range_covered(upstream_ref, effective_start, effective_end):
            return True, ""
        return False, f"upstream {upstream_ref} not fully fresh for overall interval"

    return True, ""


def check_all_upstream_fresh(
    downstream_interval: TInterval,
    downstream_overall: TInterval,
    freshness_constraints: List[TFreshnessConstraint],
    all_jobs: Dict[str, TJobDefinition],
    store: IntervalStore,
) -> Tuple[bool, List[str]]:
    """Check ALL freshness constraints. Returns (all_fresh, reasons).

    Args:
        triggers: Freshness constraints from job_def["freshness"].
    """
    reasons: List[str] = []

    for constraint in freshness_constraints:
        try:
            parsed = parse_freshness_constraint(constraint)
        except ValueError:
            reasons.append(f"invalid freshness constraint: {constraint!r}")
            continue
        upstream_ref = str(parsed.expr)
        upstream_job = all_jobs.get(upstream_ref)
        if upstream_job is None:
            reasons.append(f"upstream {upstream_ref} not found")
            continue
        upstream_spec = upstream_job.get("interval")
        if upstream_spec is None:
            reasons.append(f"upstream {upstream_ref} has no interval spec")
            continue
        # resolve upstream spec using its schedule trigger's cron
        us_cron = maybe_parse_schedule(upstream_job)
        if us_cron is None:
            reasons.append(f"upstream {upstream_ref} has no schedule trigger")
            continue
        upstream_overall = resolve_interval_spec(upstream_spec, us_cron)
        fresh, reason = check_upstream_freshness(
            downstream_interval,
            downstream_overall,
            upstream_ref,
            upstream_overall,
            parsed.type,
            store,
        )
        if not fresh:
            reasons.append(reason)

    return len(reasons) == 0, reasons
