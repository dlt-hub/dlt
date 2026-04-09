"""Tests for interval computation, eligibility, and upstream freshness checks."""

from datetime import datetime  # noqa: I251
from typing import Dict, List, Optional, Tuple

import pytest

from dlt.common.pendulum import pendulum
from dlt.common.time import ensure_pendulum_datetime_utc

from dlt._workspace.deployment.interval import (
    TInterval,
    check_all_upstream_interval_fresh,
    check_all_upstream_run_fresh,
    compute_run_interval,
    cron_floor,
    get_eligible_intervals,
    iter_intervals,
    next_eligible_interval,
    next_scheduled_run,
    resolve_interval_freshness_checks,
    resolve_interval_spec,
    sort_and_coalesce,
)
from dlt._workspace._runner.interval_store import DuckDBIntervalStore
from dlt._workspace.deployment.freshness import (
    get_direct_freshness_downstream,
    get_transitive_freshness_downstream,
)
from dlt._workspace.deployment.typing import (
    TEntryPoint,
    TExecuteSpec,
    TFreshnessConstraint,
    TIntervalSpec,
    TJobDefinition,
    TJobRef,
    TJobType,
    TTrigger,
)


def _dt(s: str) -> pendulum.DateTime:
    return ensure_pendulum_datetime_utc(s)


def _iv(start: str, end: str) -> TInterval:
    return (_dt(start), _dt(end))


def _job(
    ref: str,
    triggers: Optional[List[str]] = None,
    interval: Optional[TIntervalSpec] = None,
    default_trigger: Optional[str] = None,
    job_type: TJobType = "batch",
    freshness: Optional[List[str]] = None,
) -> TJobDefinition:
    job: TJobDefinition = {
        "job_ref": TJobRef(ref),
        "entry_point": TEntryPoint(module="m", function="f", job_type=job_type),
        "triggers": [TTrigger(t) for t in (triggers or [])],
        "execute": TExecuteSpec(),
    }
    if interval is not None:
        job["interval"] = interval
    if default_trigger is not None:
        job["default_trigger"] = TTrigger(default_trigger)
    if freshness is not None:
        job["freshness"] = [TFreshnessConstraint(c) for c in freshness]
    return job


# sort_and_coalesce


@pytest.mark.parametrize(
    "intervals,expected",
    [
        # empty
        ([], []),
        # single
        ([("2024-01-01", "2024-01-02")], [("2024-01-01", "2024-01-02")]),
        # adjacent → merged
        (
            [("2024-01-01", "2024-01-02"), ("2024-01-02", "2024-01-03")],
            [("2024-01-01", "2024-01-03")],
        ),
        # overlapping → merged
        (
            [("2024-01-01", "2024-01-03"), ("2024-01-02", "2024-01-04")],
            [("2024-01-01", "2024-01-04")],
        ),
        # gap preserved
        (
            [("2024-01-01", "2024-01-02"), ("2024-01-04", "2024-01-05")],
            [("2024-01-01", "2024-01-02"), ("2024-01-04", "2024-01-05")],
        ),
        # unsorted input
        (
            [("2024-01-04", "2024-01-05"), ("2024-01-01", "2024-01-02")],
            [("2024-01-01", "2024-01-02"), ("2024-01-04", "2024-01-05")],
        ),
        # three adjacent → single
        (
            [
                ("2024-01-01", "2024-01-02"),
                ("2024-01-02", "2024-01-03"),
                ("2024-01-03", "2024-01-04"),
            ],
            [("2024-01-01", "2024-01-04")],
        ),
    ],
    ids=["empty", "single", "adjacent", "overlapping", "gap", "unsorted", "three-adjacent"],
)
def test_sort_and_coalesce(
    intervals: List[Tuple[str, str]], expected: List[Tuple[str, str]]
) -> None:
    ivs = [(_dt(s), _dt(e)) for s, e in intervals]
    exp = [(_dt(s), _dt(e)) for s, e in expected]
    assert sort_and_coalesce(ivs) == exp


# iter_intervals


@pytest.mark.parametrize(
    "cron,overall,expected_count,first_start,first_end",
    [
        ("0 0 * * *", ("2024-01-01", "2024-01-05"), 4, "2024-01-01", "2024-01-02"),
        (
            "0 * * * *",
            ("2024-01-01T00:00:00Z", "2024-01-01T03:00:00Z"),
            3,
            "2024-01-01T00:00:00Z",
            "2024-01-01T01:00:00Z",
        ),
        ("0 0 * * *", ("2024-01-01", "2024-01-01"), 0, None, None),
        ("0 0 * * *", ("2024-01-05", "2024-01-01"), 0, None, None),
        ("0 0 1 * *", ("2024-01-01", "2024-05-01"), 4, "2024-01-01", "2024-02-01"),
    ],
    ids=["daily", "hourly", "zero-length", "inverted", "monthly-variable"],
)
def test_iter_intervals(
    cron: str,
    overall: Tuple[str, str],
    expected_count: int,
    first_start: Optional[str],
    first_end: Optional[str],
) -> None:
    iv = _iv(*overall)
    intervals = list(iter_intervals(cron, iv))
    assert len(intervals) == expected_count
    if first_start is not None:
        assert intervals[0] == (_dt(first_start), _dt(first_end))


def test_monthly_variable_length_intervals() -> None:
    """Monthly cron produces intervals of variable length (28-31 days)."""
    overall = _iv("2024-01-01", "2024-05-01")
    intervals = list(iter_intervals("0 0 1 * *", overall))
    lengths = [(iv[1] - iv[0]).days for iv in intervals]
    assert lengths == [31, 29, 31, 30]


def test_weekly_cron() -> None:
    """Weekly cron (every Monday at midnight)."""
    overall = _iv("2024-01-01", "2024-01-29")
    intervals = list(iter_intervals("0 0 * * 1", overall))
    assert len(intervals) == 4
    for iv in intervals:
        assert (iv[1] - iv[0]).days == 7


def test_iter_intervals_is_lazy() -> None:
    overall = _iv("2024-01-01", "2024-12-31")
    gen = iter_intervals("0 0 * * *", overall)
    first = next(gen)
    assert first == (_dt("2024-01-01"), _dt("2024-01-02"))
    second = next(gen)
    assert second == (_dt("2024-01-02"), _dt("2024-01-03"))


# resolve_interval_spec + cron_floor


def test_resolve_with_explicit_end() -> None:
    spec: TIntervalSpec = {"start": "2024-01-01T00:00:00Z", "end": "2024-01-05T00:00:00Z"}
    result = resolve_interval_spec(spec, "0 0 * * *")
    assert result == (_dt("2024-01-01"), _dt("2024-01-05"))


def test_resolve_open_ended_uses_last_cron_tick() -> None:
    """Open-ended spec resolves end to the last elapsed cron tick."""
    spec: TIntervalSpec = {"start": "2020-01-01T00:00:00Z"}
    start, end = resolve_interval_spec(spec, "0 0 * * *")
    assert start == _dt("2020-01-01")
    assert end <= pendulum.now("UTC")
    assert end.hour == 0 and end.minute == 0


def test_resolve_interval_spec_snaps_start() -> None:
    """When start is between cron ticks, it snaps backward."""
    spec: TIntervalSpec = {"start": "2024-01-01T06:30:00Z", "end": "2024-01-05T00:00:00Z"}
    start, end = resolve_interval_spec(spec, "0 0 * * *")
    assert start == _dt("2024-01-01")
    assert end == _dt("2024-01-05")


def test_resolve_explicit_end_snapped() -> None:
    """Explicit end between cron ticks snaps backward."""
    spec: TIntervalSpec = {"start": "2024-01-01T00:00:00Z", "end": "2024-01-05T06:30:00Z"}
    start, end = resolve_interval_spec(spec, "0 0 * * *")
    assert start == _dt("2024-01-01")
    assert end == _dt("2024-01-05")


@pytest.mark.parametrize(
    "cron_expr,dt,expected",
    [
        ("0 0 * * *", "2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z"),
        ("0 0 * * *", "2024-01-01T06:30:00Z", "2024-01-01T00:00:00Z"),
        ("*/3 * * * *", "2024-01-01T11:40:00Z", "2024-01-01T11:39:00Z"),
        ("*/3 * * * *", "2024-01-01T11:42:00Z", "2024-01-01T11:42:00Z"),
        ("* * * * *", "2024-01-01T11:40:00Z", "2024-01-01T11:40:00Z"),
    ],
    ids=["daily-aligned", "daily-misaligned", "3min-misaligned", "3min-aligned", "1min-aligned"],
)
def test_cron_floor(cron_expr: str, dt: str, expected: str) -> None:
    assert cron_floor(cron_expr, _dt(dt)) == _dt(expected)


def test_eligible_intervals_skips_completed() -> None:
    completed = sort_and_coalesce(
        [
            (_dt("2024-01-01"), _dt("2024-01-02")),
            (_dt("2024-01-02"), _dt("2024-01-03")),
        ]
    )
    overall = _iv("2024-01-01", "2024-01-05")
    eligible = get_eligible_intervals("0 0 * * *", overall, completed)
    assert len(eligible) == 2
    assert eligible[0][0] == _dt("2024-01-03")


def test_eligible_intervals_all_when_none_completed() -> None:
    overall = _iv("2024-01-01", "2024-01-05")
    eligible = get_eligible_intervals("0 0 * * *", overall, [])
    assert len(eligible) == 4


def test_eligible_intervals_ordered() -> None:
    overall = _iv("2024-01-01", "2024-01-04")
    eligible = get_eligible_intervals("0 0 * * *", overall, [])
    starts = [iv[0] for iv in eligible]
    assert starts == sorted(starts)


def test_next_eligible_interval_returns_first_incomplete() -> None:
    completed = [(_dt("2024-01-01"), _dt("2024-01-02"))]
    overall = _iv("2024-01-01", "2024-01-05")
    iv = next_eligible_interval("0 0 * * *", overall, completed)
    assert iv is not None
    assert iv[0] == _dt("2024-01-02")


def test_next_eligible_interval_none_when_all_done() -> None:
    completed = [(_dt("2024-01-01"), _dt("2024-01-03"))]
    overall = _iv("2024-01-01", "2024-01-03")
    iv = next_eligible_interval("0 0 * * *", overall, completed)
    assert iv is None


def test_next_eligible_skips_leading_completed() -> None:
    """Leading completed block is trimmed, avoiding iteration over 100 done intervals."""
    completed = [(_dt("2024-01-01"), _dt("2024-04-10"))]
    overall = _iv("2024-01-01", "2024-06-01")
    iv = next_eligible_interval("0 0 * * *", overall, completed)
    assert iv is not None
    assert iv[0] == _dt("2024-04-10")


def test_next_eligible_with_gap_in_middle() -> None:
    """Completed intervals with a gap — returns the first interval in the gap."""
    completed = sort_and_coalesce(
        [
            (_dt("2024-01-01"), _dt("2024-01-03")),
            (_dt("2024-01-04"), _dt("2024-01-05")),
        ]
    )
    overall = _iv("2024-01-01", "2024-01-05")
    iv = next_eligible_interval("0 0 * * *", overall, completed)
    assert iv is not None
    assert iv == (_dt("2024-01-03"), _dt("2024-01-04"))


@pytest.mark.parametrize(
    "trigger,tz,prev,now_ref,expected_at",
    [
        # schedule: next cron tick after now
        (
            "schedule:0 0 * * *",
            "UTC",
            None,
            "2024-06-15T12:00:00Z",
            "2024-06-16T00:00:00Z",
        ),
        # schedule: exactly on a tick — next is the following tick
        (
            "schedule:0 0 * * *",
            "UTC",
            None,
            "2024-06-15T00:00:00Z",
            "2024-06-16T00:00:00Z",
        ),
        # schedule: with timezone
        (
            "schedule:0 8 * * *",
            "US/Eastern",
            None,
            "2024-06-15T11:00:00Z",  # 07:00 ET, before 08:00
            "2024-06-15T12:00:00Z",  # 08:00 ET
        ),
        # every: first run (no prev), waits one period
        (
            "every:1h",
            "UTC",
            None,
            "2024-06-15T10:30:00Z",
            "2024-06-15T11:30:00Z",
        ),
        # every: prev exists, next = prev + period
        (
            "every:1h",
            "UTC",
            "2024-06-15T10:00:00Z",
            "2024-06-15T10:30:00Z",
            "2024-06-15T11:00:00Z",
        ),
        # every: prev + period is in the past — clamp to now + period
        (
            "every:1h",
            "UTC",
            "2024-06-15T08:00:00Z",
            "2024-06-15T12:00:00Z",
            "2024-06-15T13:00:00Z",
        ),
        # once: future datetime
        (
            "once:2024-12-31T23:59:59Z",
            "UTC",
            None,
            "2024-06-15T00:00:00Z",
            "2024-12-31T23:59:59Z",
        ),
        # once: past datetime — clamp to now
        (
            "once:2024-01-01T00:00:00Z",
            "UTC",
            None,
            "2024-06-15T00:00:00Z",
            "2024-06-15T00:00:00Z",
        ),
    ],
    ids=[
        "schedule-midday",
        "schedule-on-tick",
        "schedule-timezone",
        "every-first-waits-period",
        "every-with-prev",
        "every-clamp-to-now-plus-period",
        "once-future",
        "once-past-clamped",
    ],
)
def test_next_scheduled_run(
    trigger: str,
    tz: str,
    prev: Optional[str],
    now_ref: str,
    expected_at: str,
) -> None:
    scheduled_at = next_scheduled_run(
        TTrigger(trigger),
        _dt(now_ref),
        tz=tz,
        prev_scheduled_run=_dt(prev) if prev else None,
    )
    assert scheduled_at == _dt(expected_at)


def test_next_scheduled_run_returns_utc() -> None:
    """All returned datetimes are UTC regardless of tz parameter."""
    scheduled_at = next_scheduled_run(
        TTrigger("schedule:0 8 * * *"),
        _dt("2024-06-15T11:00:00Z"),
        tz="US/Eastern",
    )
    assert scheduled_at.tzname() == "UTC"

    scheduled_every = next_scheduled_run(
        TTrigger("every:1h"),
        _dt("2024-06-15T10:00:00Z"),
    )
    assert scheduled_every.tzname() == "UTC"

    scheduled_once = next_scheduled_run(
        TTrigger("once:2025-01-01T00:00:00Z"),
        _dt("2024-06-15T00:00:00Z"),
    )
    assert scheduled_once.tzname() == "UTC"


def test_next_scheduled_run_rejects_non_timed() -> None:
    """Non-timed triggers raise InvalidTrigger."""
    from dlt._workspace.deployment.exceptions import InvalidTrigger as IT

    with pytest.raises(IT, match="not a timed trigger"):
        next_scheduled_run(TTrigger("manual:jobs.mod.a"), _dt("2024-06-15T00:00:00Z"))


@pytest.mark.parametrize(
    "trigger,prev,now,expected_start,expected_end",
    [
        # prev=None synthesizes start by trigger type
        # schedule: previous cron tick
        (
            "schedule:0 * * * *",
            None,
            "2024-06-15T12:30:00Z",
            "2024-06-15T12:00:00Z",
            "2024-06-15T12:30:00Z",
        ),
        # schedule: now exactly on a tick — that tick is the start (cron_floor includes equality)
        (
            "schedule:0 * * * *",
            None,
            "2024-06-15T12:00:00Z",
            "2024-06-15T12:00:00Z",
            "2024-06-15T12:00:00Z",
        ),
        # every: now - period
        (
            "every:1h",
            None,
            "2024-06-15T12:30:00Z",
            "2024-06-15T11:30:00Z",
            "2024-06-15T12:30:00Z",
        ),
        # every: 5-minute period
        (
            "every:5m",
            None,
            "2024-06-15T12:30:00Z",
            "2024-06-15T12:25:00Z",
            "2024-06-15T12:30:00Z",
        ),
        # once: zero-length at the once time, regardless of now
        (
            "once:2030-01-01T00:00:00Z",
            None,
            "2024-06-15T12:30:00Z",
            "2030-01-01T00:00:00Z",
            "2030-01-01T00:00:00Z",
        ),
        # manual: zero-length at now
        (
            "manual:jobs.mod.a",
            None,
            "2024-06-15T12:30:00Z",
            "2024-06-15T12:30:00Z",
            "2024-06-15T12:30:00Z",
        ),
        # tag: zero-length at now
        (
            "tag:nightly",
            None,
            "2024-06-15T12:30:00Z",
            "2024-06-15T12:30:00Z",
            "2024-06-15T12:30:00Z",
        ),
        # job.success: zero-length at now
        (
            "job.success:jobs.mod.upstream",
            None,
            "2024-06-15T12:30:00Z",
            "2024-06-15T12:30:00Z",
            "2024-06-15T12:30:00Z",
        ),
        # prev set + schedule → prev wins, end is now (not the cron tick)
        (
            "schedule:0 * * * *",
            "2024-06-15T10:15:23Z",
            "2024-06-15T12:30:00Z",
            "2024-06-15T10:15:23Z",
            "2024-06-15T12:30:00Z",
        ),
        # prev set + every → prev wins
        (
            "every:1h",
            "2024-06-15T10:00:00Z",
            "2024-06-15T12:30:00Z",
            "2024-06-15T10:00:00Z",
            "2024-06-15T12:30:00Z",
        ),
        # prev set + once → prev wins (overrides zero-length default)
        (
            "once:2030-01-01T00:00:00Z",
            "2024-06-15T10:00:00Z",
            "2024-06-15T12:30:00Z",
            "2024-06-15T10:00:00Z",
            "2024-06-15T12:30:00Z",
        ),
        # prev set + manual → prev wins
        (
            "manual:jobs.mod.a",
            "2024-06-15T10:00:00Z",
            "2024-06-15T12:30:00Z",
            "2024-06-15T10:00:00Z",
            "2024-06-15T12:30:00Z",
        ),
    ],
    ids=[
        "schedule-prev=None-midhour",
        "schedule-prev=None-on-tick",
        "every-prev=None-1h",
        "every-prev=None-5m",
        "once-prev=None",
        "manual-prev=None",
        "tag-prev=None",
        "job.success-prev=None",
        "schedule-prev-set",
        "every-prev-set",
        "once-prev-set",
        "manual-prev-set",
    ],
)
def test_compute_run_interval(
    trigger: str,
    prev: Optional[str],
    now: str,
    expected_start: str,
    expected_end: str,
) -> None:
    iv = compute_run_interval(
        TTrigger(trigger),
        _dt(now),
        prev_completed_run=_dt(prev) if prev else None,
    )
    assert iv == (_dt(expected_start), _dt(expected_end))


def test_compute_run_interval_returns_utc() -> None:
    """All returned datetimes are UTC."""
    iv = compute_run_interval(
        TTrigger("schedule:0 8 * * *"),
        _dt("2024-06-15T11:00:00Z"),
        prev_completed_run=None,
        tz="US/Eastern",
    )
    assert iv[0].tzname() == "UTC"
    assert iv[1].tzname() == "UTC"


def test_compute_run_interval_schedule_with_timezone() -> None:
    """Cron tick is computed in target tz then converted to UTC."""
    # 08:00 ET = 12:00 UTC (DST). At now=11:00 UTC (07:00 ET), the previous
    # 08:00 ET tick is yesterday's 08:00 ET = previous day 12:00 UTC.
    iv = compute_run_interval(
        TTrigger("schedule:0 8 * * *"),
        _dt("2024-06-15T11:00:00Z"),
        prev_completed_run=None,
        tz="US/Eastern",
    )
    assert iv == (_dt("2024-06-14T12:00:00Z"), _dt("2024-06-15T11:00:00Z"))


def test_compute_run_interval_invalid_trigger() -> None:
    """Unparseable triggers raise InvalidTrigger."""
    from dlt._workspace.deployment.exceptions import InvalidTrigger as IT

    with pytest.raises(IT):
        compute_run_interval(
            TTrigger("not-a-trigger"),
            _dt("2024-06-15T12:00:00Z"),
            prev_completed_run=None,
        )


# freshness checks


def _check_interval_freshness(
    ds_iv: TInterval,
    ds_overall: TInterval,
    upstream_job: TJobDefinition,
    store: DuckDBIntervalStore,
    constraint: str = "job.is_matching_interval_fresh",
) -> Tuple[bool, List[str]]:
    """Resolve + fetch + evaluate interval freshness for a single upstream."""
    ref = upstream_job["job_ref"]
    all_jobs: Dict[str, TJobDefinition] = {ref: upstream_job}
    freshness = [TFreshnessConstraint(f"{constraint}:{ref}")]
    checks, reasons = resolve_interval_freshness_checks(ds_iv, ds_overall, freshness, all_jobs)
    if not reasons:
        completions = {
            (c.upstream_ref, c.effective_interval): store.is_interval_completed(
                c.upstream_ref, c.effective_interval
            )
            for c in checks
        }
        _, reasons = check_all_upstream_interval_fresh(checks, completions)
    return len(reasons) == 0, reasons


@pytest.mark.parametrize(
    "ds_iv,us_interval_spec,constraint,completed,expected",
    [
        (
            ("2024-01-01", "2024-01-02"),
            {"start": "2024-01-01T00:00:00Z", "end": "2024-01-31T00:00:00Z"},
            "job.is_matching_interval_fresh",
            [("2024-01-01", "2024-01-02")],
            True,
        ),
        (
            ("2024-01-01", "2024-01-02"),
            {"start": "2024-01-01T00:00:00Z", "end": "2024-01-31T00:00:00Z"},
            "job.is_matching_interval_fresh",
            [],
            False,
        ),
        (
            ("2024-06-01", "2024-06-02"),
            {"start": "2025-01-01T00:00:00Z", "end": "2025-12-31T00:00:00Z"},
            "job.is_matching_interval_fresh",
            [],
            True,
        ),
        (
            ("2024-01-15", "2024-01-16"),
            {"start": "2024-01-01T00:00:00Z", "end": "2024-01-31T00:00:00Z"},
            "job.is_fresh",
            [("2024-01-01", "2024-01-31")],
            True,
        ),
        (
            ("2024-01-15", "2024-01-16"),
            {"start": "2024-01-01T00:00:00Z", "end": "2024-01-31T00:00:00Z"},
            "job.is_fresh",
            [("2024-01-01", "2024-01-15")],
            False,
        ),
        (
            ("2024-06-01", "2024-06-02"),
            {"start": "2025-01-01T00:00:00Z", "end": "2025-06-01T00:00:00Z"},
            "job.is_fresh",
            [("2025-01-01", "2025-06-01")],
            True,
        ),
        (
            ("2024-02-01", "2024-02-02"),
            {"start": "2024-01-01T00:00:00Z", "end": "2024-01-15T00:00:00Z"},
            "job.is_matching_interval_fresh",
            [("2024-01-01", "2024-01-15")],
            False,
        ),
    ],
    ids=[
        "matching-covered",
        "matching-gap",
        "matching-non-overlapping",
        "fresh-complete",
        "fresh-incomplete",
        "fresh-non-overlapping",
        "matching-after-upstream-end",
    ],
)
def test_check_upstream_freshness(
    ds_iv: Tuple[str, str],
    us_interval_spec: TIntervalSpec,
    constraint: str,
    completed: List[Tuple[str, str]],
    expected: bool,
) -> None:
    store = DuckDBIntervalStore()
    for s, e in completed:
        store.mark_interval_completed("jobs.up", (_dt(s), _dt(e)))
    ds_overall = _iv("2024-01-01", "2025-06-01")
    up = _job(
        "jobs.up",
        ["schedule:0 0 * * *"],
        interval=us_interval_spec,
        default_trigger="schedule:0 0 * * *",
    )
    fresh, _ = _check_interval_freshness(_iv(*ds_iv), ds_overall, up, store, constraint)
    assert fresh == expected
    store.close()


def _resolve_and_check_interval_freshness(
    ds_iv: TInterval,
    ds_overall: TInterval,
    freshness: List[TFreshnessConstraint],
    all_jobs: Dict[str, TJobDefinition],
    store: DuckDBIntervalStore,
) -> Tuple[bool, List[str]]:
    """Full resolve + fetch + evaluate for interval freshness."""
    checks, reasons = resolve_interval_freshness_checks(ds_iv, ds_overall, freshness, all_jobs)
    if not reasons:
        completions = {
            (c.upstream_ref, c.effective_interval): store.is_interval_completed(
                c.upstream_ref, c.effective_interval
            )
            for c in checks
        }
        _, reasons = check_all_upstream_interval_fresh(checks, completions)
    return len(reasons) == 0, reasons


def test_all_upstream_must_pass() -> None:
    """One incomplete upstream blocks the downstream."""
    store = DuckDBIntervalStore()
    store.mark_interval_completed("jobs.a", (_dt("2024-01-01"), _dt("2024-01-02")))
    up_a = _job(
        "jobs.a",
        ["schedule:0 0 * * *"],
        interval={"start": "2024-01-01T00:00:00Z", "end": "2024-01-31T00:00:00Z"},
        default_trigger="schedule:0 0 * * *",
    )
    up_b = _job(
        "jobs.b",
        ["schedule:0 0 * * *"],
        interval={"start": "2024-01-01T00:00:00Z", "end": "2024-01-31T00:00:00Z"},
        default_trigger="schedule:0 0 * * *",
    )
    all_jobs = {"jobs.a": up_a, "jobs.b": up_b}
    freshness = [
        TFreshnessConstraint("job.is_matching_interval_fresh:jobs.a"),
        TFreshnessConstraint("job.is_matching_interval_fresh:jobs.b"),
    ]
    fresh, reasons = _resolve_and_check_interval_freshness(
        _iv("2024-01-01", "2024-01-02"),
        _iv("2024-01-01", "2024-01-31"),
        freshness,
        all_jobs,
        store,
    )
    assert not fresh
    assert len(reasons) == 1
    assert "jobs.b" in reasons[0]
    store.close()


def test_all_upstream_fresh_when_all_covered() -> None:
    store = DuckDBIntervalStore()
    store.mark_interval_completed("jobs.a", (_dt("2024-01-01"), _dt("2024-01-02")))
    store.mark_interval_completed("jobs.b", (_dt("2024-01-01"), _dt("2024-01-02")))
    up_a = _job(
        "jobs.a",
        ["schedule:0 0 * * *"],
        interval={"start": "2024-01-01T00:00:00Z", "end": "2024-01-31T00:00:00Z"},
        default_trigger="schedule:0 0 * * *",
    )
    up_b = _job(
        "jobs.b",
        ["schedule:0 0 * * *"],
        interval={"start": "2024-01-01T00:00:00Z", "end": "2024-01-31T00:00:00Z"},
        default_trigger="schedule:0 0 * * *",
    )
    all_jobs = {"jobs.a": up_a, "jobs.b": up_b}
    freshness = [
        TFreshnessConstraint("job.is_matching_interval_fresh:jobs.a"),
        TFreshnessConstraint("job.is_matching_interval_fresh:jobs.b"),
    ]
    fresh, reasons = _resolve_and_check_interval_freshness(
        _iv("2024-01-01", "2024-01-02"),
        _iv("2024-01-01", "2024-01-31"),
        freshness,
        all_jobs,
        store,
    )
    assert fresh
    assert reasons == []
    store.close()


def test_freshness_with_misaligned_cron_schedules() -> None:
    """Downstream */1 intervals within upstream */3 interval require completion."""
    store = DuckDBIntervalStore()
    up = _job(
        "jobs.up",
        ["schedule:*/3 * * * *"],
        interval={"start": "2024-01-01T11:40:00Z", "end": "2024-01-01T12:00:00Z"},
        default_trigger="schedule:*/3 * * * *",
    )
    ds_overall = _iv("2024-01-01T11:40:00Z", "2024-01-01T12:00:00Z")

    # [11:40, 11:41) within upstream [11:39, 11:42) → not fresh yet
    fresh, _ = _check_interval_freshness(
        _iv("2024-01-01T11:40:00Z", "2024-01-01T11:41:00Z"),
        ds_overall,
        up,
        store,
    )
    assert not fresh

    # complete upstream [11:39, 11:42) → downstream [11:40, 11:41) now fresh
    store.mark_interval_completed(
        "jobs.up", (_dt("2024-01-01T11:39:00Z"), _dt("2024-01-01T11:42:00Z"))
    )
    fresh, _ = _check_interval_freshness(
        _iv("2024-01-01T11:40:00Z", "2024-01-01T11:41:00Z"),
        ds_overall,
        up,
        store,
    )
    assert fresh

    # [11:42, 11:43) not fresh until upstream [11:42, 11:45) completes
    fresh, _ = _check_interval_freshness(
        _iv("2024-01-01T11:42:00Z", "2024-01-01T11:43:00Z"),
        ds_overall,
        up,
        store,
    )
    assert not fresh

    store.mark_interval_completed(
        "jobs.up", (_dt("2024-01-01T11:42:00Z"), _dt("2024-01-01T11:45:00Z"))
    )
    fresh, _ = _check_interval_freshness(
        _iv("2024-01-01T11:42:00Z", "2024-01-01T11:43:00Z"),
        ds_overall,
        up,
        store,
    )
    assert fresh

    store.close()


def test_freshness_via_resolve_and_check_with_misaligned_cron() -> None:
    """resolve + check resolves upstream overall with cron_floor snapping."""
    store = DuckDBIntervalStore()
    up = _job(
        "jobs.up",
        ["schedule:*/3 * * * *"],
        interval={"start": "2024-01-01T11:40:00Z", "end": "2024-01-01T12:00:00Z"},
        default_trigger="schedule:*/3 * * * *",
    )
    all_jobs: Dict[str, TJobDefinition] = {"jobs.up": up}
    freshness = [TFreshnessConstraint("job.is_matching_interval_fresh:jobs.up")]
    ds_overall = _iv("2024-01-01T11:40:00Z", "2024-01-01T12:00:00Z")

    fresh, _ = _resolve_and_check_interval_freshness(
        _iv("2024-01-01T11:40:00Z", "2024-01-01T11:41:00Z"),
        ds_overall,
        freshness,
        all_jobs,
        store,
    )
    assert not fresh

    store.mark_interval_completed(
        "jobs.up", (_dt("2024-01-01T11:39:00Z"), _dt("2024-01-01T11:42:00Z"))
    )
    fresh, reasons = _resolve_and_check_interval_freshness(
        _iv("2024-01-01T11:40:00Z", "2024-01-01T11:41:00Z"),
        ds_overall,
        freshness,
        all_jobs,
        store,
    )
    assert fresh, f"should be fresh: {reasons}"

    store.close()


# generalized freshness — run-based checks (schedule without interval, every, event, interactive)


def _check_run_freshness(
    upstream_job: TJobDefinition,
    prev: Optional[pendulum.DateTime],
    constraint: str = "job.is_fresh",
) -> Tuple[bool, List[str]]:
    """Run check_all_upstream_run_fresh with a single upstream."""
    ref = upstream_job["job_ref"]
    all_jobs: Dict[str, TJobDefinition] = {ref: upstream_job}
    freshness = [TFreshnessConstraint(f"{constraint}:{ref}")]
    return check_all_upstream_run_fresh(freshness, all_jobs, {ref: prev})


@pytest.mark.parametrize(
    "trigger,default_trigger,prev_offset,expected_fresh,reason_frag",
    [
        # schedule: prev covers the current cron tick → fresh
        ("schedule:0 * * * *", "schedule:0 * * * *", "current_tick", True, None),
        # schedule: prev covers the previous cron tick (the most recently completed
        # period) → fresh under the relaxed semantic
        ("schedule:0 * * * *", "schedule:0 * * * *", "previous_tick", True, None),
        # schedule: prev is two ticks behind → stale
        ("schedule:0 * * * *", "schedule:0 * * * *", "two_ticks_behind", False, "missing run"),
        # schedule: no prev_completed_run → not in usable state
        ("schedule:0 * * * *", "schedule:0 * * * *", None, False, "no usable completion"),
        # every:5m — prev 2m ago → fresh (within current period)
        ("every:5m", "every:5m", "recent", True, None),
        # every:5m — prev 8m ago → fresh (within previous period)
        ("every:5m", "every:5m", "previous_period", True, None),
        # every:5m — prev 12m ago → stale (older than 2 periods)
        ("every:5m", "every:5m", "two_periods_old", False, "missing run"),
        # every:5m — no prev_completed_run → not in usable state
        ("every:5m", "every:5m", None, False, "no usable completion"),
        # event — prev set → fresh
        ("job.success:jobs.other", "job.success:jobs.other", "recent", True, None),
        # event — no prev_completed_run → not in usable state
        ("job.success:jobs.other", "job.success:jobs.other", None, False, "no usable completion"),
    ],
    ids=[
        "schedule-no-iv-fresh",
        "schedule-no-iv-previous-tick",
        "schedule-no-iv-two-ticks-behind",
        "schedule-no-iv-no-prev",
        "every-recent-fresh",
        "every-previous-period",
        "every-two-periods-old",
        "every-no-prev",
        "event-prev-set",
        "event-no-prev",
    ],
)
def test_run_based_freshness(
    trigger: str,
    default_trigger: str,
    prev_offset: Optional[str],
    expected_fresh: bool,
    reason_frag: Optional[str],
) -> None:
    """Parametrized test for run-based freshness: schedule-no-interval, every, event."""
    prev: Optional[pendulum.DateTime] = None

    if prev_offset is not None:
        now: pendulum.DateTime = pendulum.now("UTC")
        if prev_offset == "current_tick":
            prev = pendulum.instance(cron_floor("0 * * * *", now))
        elif prev_offset == "previous_tick":
            # one cron tick before the current floor (the most recently completed period)
            prev = pendulum.instance(cron_floor("0 * * * *", now)).subtract(hours=1)
        elif prev_offset == "two_ticks_behind":
            prev = pendulum.instance(cron_floor("0 * * * *", now)).subtract(hours=2)
        elif prev_offset == "recent":
            prev = now.subtract(minutes=2)
        elif prev_offset == "previous_period":
            # within 2*period: relaxed check still considers fresh
            prev = now.subtract(minutes=8)
        elif prev_offset == "two_periods_old":
            # > 2*period: stale
            prev = now.subtract(minutes=12)
        else:
            prev = now

    up = _job("jobs.up", [trigger], default_trigger=default_trigger)
    fresh, reasons = _check_run_freshness(up, prev)

    assert fresh == expected_fresh
    if reason_frag:
        assert any(reason_frag in r for r in reasons), f"expected '{reason_frag}' in {reasons}"


def test_freshness_interactive_upstream_always_not_fresh() -> None:
    """Interactive upstream cannot be fresh."""
    up = _job("jobs.up", ["http:"], default_trigger="http:", job_type="interactive")
    fresh, reasons = _check_run_freshness(up, None)
    assert not fresh
    assert "interactive" in reasons[0]


def test_schedule_freshness_handles_minute_boundary_crossing() -> None:
    """Regression: a job that started at 22:49:55 and is checked at 22:50:01 should be fresh.

    Without the relaxed check (`expected = previous tick`), this would fail because
    the upstream's `prev_completed_run` is anchored to the 22:49 tick (start of the
    minute it ran in) while the strict `expected = current tick` would be 22:50.
    """
    # prev_completed_run = cron_floor of started_at = 22:49:00
    prev = ensure_pendulum_datetime_utc("2026-04-08T22:49:00Z")
    # downstream is checking ~6s later, after the minute has rolled over
    check_now = ensure_pendulum_datetime_utc("2026-04-08T22:50:01Z")
    import dlt._workspace.deployment.interval as iv_mod

    orig_now = iv_mod.pendulum.now
    try:
        iv_mod.pendulum.now = lambda *a, **kw: check_now
        fresh, reason = _check_run_freshness(
            _job("jobs.up", ["schedule:* * * * *"], default_trigger="schedule:* * * * *"),
            prev,
        )
    finally:
        iv_mod.pendulum.now = orig_now
    assert fresh, f"expected fresh, got reason: {reason}"


@pytest.mark.parametrize(
    "trigger,default_trigger",
    [
        ("every:5m", "every:5m"),
        ("schedule:0 * * * *", "schedule:0 * * * *"),
    ],
    ids=["every-no-interval", "schedule-no-interval"],
)
def test_matching_interval_fresh_on_non_interval_upstream_fails(
    trigger: str, default_trigger: str
) -> None:
    """is_matching_interval_fresh requires upstream to have schedule + interval."""
    up = _job("jobs.up", [trigger], default_trigger=default_trigger)
    fresh, reasons = _check_run_freshness(up, None, constraint="job.is_matching_interval_fresh")
    assert not fresh
    assert "job.is_matching_interval_fresh requires" in reasons[0]


# check_all_upstream_interval_fresh — rejection cases


@pytest.mark.parametrize(
    "trigger,default_trigger,job_type_,reason_frag",
    [
        ("http:", "http:", "interactive", "interactive"),
        ("every:5m", "every:5m", "batch", "no interval"),
        ("schedule:0 * * * *", "schedule:0 * * * *", "batch", "no interval"),
    ],
    ids=["interactive-upstream", "every-no-interval", "schedule-no-interval"],
)
def test_interval_fresh_rejects_invalid_upstream(
    trigger: str, default_trigger: str, job_type_: str, reason_frag: str
) -> None:
    """Interval freshness rejects upstreams that lack schedule+interval."""
    store = DuckDBIntervalStore()
    up = _job("jobs.up", [trigger], default_trigger=default_trigger, job_type=job_type_)  # type: ignore[arg-type]
    all_jobs: Dict[str, TJobDefinition] = {"jobs.up": up}
    freshness = [TFreshnessConstraint("job.is_fresh:jobs.up")]
    fresh, reasons = _resolve_and_check_interval_freshness(
        _iv("2024-01-01", "2024-01-02"),
        _iv("2024-01-01", "2024-01-31"),
        freshness,
        all_jobs,
        store,
    )
    assert not fresh
    assert reason_frag in reasons[0]
    store.close()


# check_all_upstream_run_fresh — rejection and edge cases


@pytest.mark.parametrize(
    "trigger,default_trigger,job_type_,constraint,has_prev,reason_frag",
    [
        # is_matching_interval_fresh rejected in run mode
        (
            "every:5m",
            "every:5m",
            "batch",
            "job.is_matching_interval_fresh",
            True,
            "requires interval",
        ),
        # interactive upstream rejected
        ("http:", "http:", "interactive", "job.is_fresh", False, "interactive"),
        # no default_trigger, no prev → event path, stale
        ("manual:jobs.up", None, "batch", "job.is_fresh", False, "no usable completion"),
        # no default_trigger, prev set → event path, fresh
        ("manual:jobs.up", None, "batch", "job.is_fresh", True, None),
    ],
    ids=[
        "rejects-is-matching-interval-fresh",
        "rejects-interactive",
        "no-trigger-no-prev",
        "no-trigger-with-prev",
    ],
)
def test_run_fresh_edge_cases(
    trigger: str,
    default_trigger: Optional[str],
    job_type_: str,
    constraint: str,
    has_prev: bool,
    reason_frag: Optional[str],
) -> None:
    """Parametrized edge cases for check_all_upstream_run_fresh."""
    prev: Optional[pendulum.DateTime] = pendulum.now("UTC") if has_prev else None
    up = _job("jobs.up", [trigger], default_trigger=default_trigger, job_type=job_type_)  # type: ignore[arg-type]
    fresh, reasons = _check_run_freshness(up, prev, constraint=constraint)
    if reason_frag is None:
        assert fresh, f"should be fresh: {reasons}"
    else:
        assert not fresh
        assert reason_frag in reasons[0]


def test_run_fresh_mixed_upstreams() -> None:
    """Multiple constraints: one passes, one fails."""
    now = pendulum.now("UTC")
    up_a = _job("jobs.a", ["every:5m"], default_trigger="every:5m")
    up_b = _job("jobs.b", ["every:5m"], default_trigger="every:5m")
    all_jobs: Dict[str, TJobDefinition] = {"jobs.a": up_a, "jobs.b": up_b}
    freshness = [
        TFreshnessConstraint("job.is_fresh:jobs.a"),
        TFreshnessConstraint("job.is_fresh:jobs.b"),
    ]
    prev_runs: Dict[str, Optional[datetime]] = {
        "jobs.a": now,
        "jobs.b": None,
    }
    fresh, reasons = check_all_upstream_run_fresh(freshness, all_jobs, prev_runs)
    assert not fresh
    assert len(reasons) == 1
    assert "jobs.b" in reasons[0]


def _make_chain(*refs: str) -> Dict[str, TJobDefinition]:
    """Linear chain a → b → c → ... where each downstream has freshness on its predecessor."""
    jobs: Dict[str, TJobDefinition] = {}
    prev: Optional[str] = None
    for ref in refs:
        jobs[ref] = _job(
            ref,
            ["manual:" + ref],
            freshness=[f"job.is_fresh:{prev}"] if prev else None,
        )
        prev = ref
    return jobs


def _diamond_jobs() -> Dict[str, TJobDefinition]:
    """a → {b, c} → d (b and c both downstream of a, d downstream of both)."""
    return {
        "jobs.a": _job("jobs.a", ["manual:jobs.a"]),
        "jobs.b": _job("jobs.b", ["manual:jobs.b"], freshness=["job.is_fresh:jobs.a"]),
        "jobs.c": _job("jobs.c", ["manual:jobs.c"], freshness=["job.is_fresh:jobs.a"]),
        "jobs.d": _job(
            "jobs.d",
            ["manual:jobs.d"],
            freshness=["job.is_fresh:jobs.b", "job.is_fresh:jobs.c"],
        ),
    }


def _self_loop_jobs() -> Dict[str, TJobDefinition]:
    """Single job with freshness pointing at itself."""
    return {
        "jobs.a": _job("jobs.a", ["manual:jobs.a"], freshness=["job.is_fresh:jobs.a"]),
    }


def _invalid_constraint_jobs() -> Dict[str, TJobDefinition]:
    """jobs.b has one malformed constraint and one valid constraint on jobs.a."""
    return {
        "jobs.a": _job("jobs.a", ["manual:jobs.a"]),
        "jobs.b": _job(
            "jobs.b",
            ["manual:jobs.b"],
            freshness=["not-a-constraint", "job.is_fresh:jobs.a"],
        ),
    }


def _cycle_jobs() -> Dict[str, TJobDefinition]:
    """Cycle a → b → c → a (each freshness points back one hop)."""
    return {
        "jobs.a": _job("jobs.a", ["manual:jobs.a"], freshness=["job.is_fresh:jobs.c"]),
        "jobs.b": _job("jobs.b", ["manual:jobs.b"], freshness=["job.is_fresh:jobs.a"]),
        "jobs.c": _job("jobs.c", ["manual:jobs.c"], freshness=["job.is_fresh:jobs.b"]),
    }


def _disconnected_jobs() -> Dict[str, TJobDefinition]:
    """Two jobs with no freshness edges between them."""
    return {
        "jobs.a": _job("jobs.a", ["manual:jobs.a"]),
        "jobs.b": _job("jobs.b", ["manual:jobs.b"]),
    }


@pytest.mark.parametrize(
    "jobs,query_ref,expected",
    [
        # chain: a → b → c
        (_make_chain("jobs.a", "jobs.b", "jobs.c"), "jobs.a", ["jobs.b"]),
        (_make_chain("jobs.a", "jobs.b", "jobs.c"), "jobs.b", ["jobs.c"]),
        (_make_chain("jobs.a", "jobs.b", "jobs.c"), "jobs.c", []),
        # self-loop: a → a is suppressed
        (_self_loop_jobs(), "jobs.a", []),
        # diamond: a → {b, c} → d
        (_diamond_jobs(), "jobs.a", ["jobs.b", "jobs.c"]),
        (_diamond_jobs(), "jobs.b", ["jobs.d"]),
        (_diamond_jobs(), "jobs.c", ["jobs.d"]),
        (_diamond_jobs(), "jobs.d", []),
        # malformed constraints are silently ignored, valid ones still match
        (_invalid_constraint_jobs(), "jobs.a", ["jobs.b"]),
        # disconnected
        (_disconnected_jobs(), "jobs.a", []),
    ],
    ids=[
        "chain-from-a",
        "chain-from-b",
        "chain-leaf",
        "self-loop",
        "diamond-from-a",
        "diamond-from-b",
        "diamond-from-c",
        "diamond-leaf",
        "invalid-constraint-ignored",
        "disconnected",
    ],
)
def test_get_direct_freshness_downstream(
    jobs: Dict[str, TJobDefinition],
    query_ref: str,
    expected: List[str],
) -> None:
    assert get_direct_freshness_downstream(query_ref, jobs) == expected


@pytest.mark.parametrize(
    "jobs,query_ref,expected",
    [
        # chain (4 nodes): full BFS order
        (
            _make_chain("jobs.a", "jobs.b", "jobs.c", "jobs.d"),
            "jobs.a",
            ["jobs.b", "jobs.c", "jobs.d"],
        ),
        # diamond: d appears exactly once even though both b and c reach it
        (_diamond_jobs(), "jobs.a", ["jobs.b", "jobs.c", "jobs.d"]),
        # cycle a → b → c → a: seed is excluded even when reachable via the cycle
        (_cycle_jobs(), "jobs.a", ["jobs.b", "jobs.c"]),
        # disconnected
        (_disconnected_jobs(), "jobs.a", []),
        # leaf in chain has no downstream
        (_make_chain("jobs.a", "jobs.b", "jobs.c"), "jobs.c", []),
    ],
    ids=["chain", "diamond", "cycle-seed-excluded", "disconnected", "leaf"],
)
def test_get_transitive_freshness_downstream(
    jobs: Dict[str, TJobDefinition],
    query_ref: str,
    expected: List[str],
) -> None:
    assert get_transitive_freshness_downstream(query_ref, jobs) == expected
