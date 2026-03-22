"""Tests for interval computation, tracking, and upstream freshness checks."""

from typing import Dict, List, Optional

import pytest

from dlt.common.pendulum import pendulum
from dlt.common.time import ensure_pendulum_datetime_utc

from dlt._workspace._runner.interval import (
    IntervalStore,
    TInterval,
    check_all_upstream_fresh,
    check_upstream_freshness,
    compute_intervals,
    get_eligible_intervals,
    iter_intervals,
    next_eligible_interval,
    resolve_interval_spec,
)
from dlt._workspace.deployment.typing import (
    TEntryPoint,
    TExecutionSpec,
    TIntervalSpec,
    TJobDefinition,
    TJobRef,
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
) -> TJobDefinition:
    job: TJobDefinition = {
        "job_ref": TJobRef(ref),
        "entry_point": TEntryPoint(module="m", function="f", job_type="batch"),
        "triggers": [TTrigger(t) for t in (triggers or [])],
        "execution": TExecutionSpec(),
        "starred": False,
    }
    if interval is not None:
        job["interval"] = interval
    return job


@pytest.mark.parametrize(
    "completed,query_start,query_end,expected",
    [
        # single interval covers exact range
        ([("2024-01-01", "2024-01-02")], "2024-01-01", "2024-01-02", True),
        # gap between two intervals
        (
            [("2024-01-01", "2024-01-02"), ("2024-01-04", "2024-01-05")],
            "2024-01-01",
            "2024-01-05",
            False,
        ),
        # adjacent intervals cover full range
        (
            [("2024-01-01", "2024-01-03"), ("2024-01-03", "2024-01-05")],
            "2024-01-01",
            "2024-01-05",
            True,
        ),
        # partial coverage
        ([("2024-01-01", "2024-01-03")], "2024-01-01", "2024-01-05", False),
        # empty store
        ([], "2024-01-01", "2024-01-02", False),
        # overlapping intervals cover range
        (
            [("2024-01-01", "2024-01-04"), ("2024-01-03", "2024-01-06")],
            "2024-01-01",
            "2024-01-06",
            True,
        ),
    ],
    ids=["exact", "gap", "adjacent", "partial", "empty", "overlapping"],
)
def test_store_is_range_covered(
    completed: List[tuple],
    query_start: str,
    query_end: str,
    expected: bool,
) -> None:
    store = IntervalStore()
    for s, e in completed:
        store.mark_completed("jobs.a", _dt(s), _dt(e))
    assert store.is_range_covered("jobs.a", _dt(query_start), _dt(query_end)) == expected
    store.close()


def test_store_multiple_jobs_isolated() -> None:
    store = IntervalStore()
    store.mark_completed("jobs.a", _dt("2024-01-01"), _dt("2024-01-05"))
    assert not store.is_range_covered("jobs.b", _dt("2024-01-01"), _dt("2024-01-05"))
    store.close()


def test_daily_covers_hourly_subinterval() -> None:
    store = IntervalStore()
    store.mark_completed("jobs.d", _dt("2024-01-15"), _dt("2024-01-16"))
    assert store.is_range_covered(
        "jobs.d", _dt("2024-01-15T08:00:00Z"), _dt("2024-01-15T09:00:00Z")
    )
    store.close()


@pytest.mark.parametrize(
    "cron,overall,expected_count,first_start,first_end",
    [
        # daily
        ("0 0 * * *", ("2024-01-01", "2024-01-05"), 4, "2024-01-01", "2024-01-02"),
        # hourly
        (
            "0 * * * *",
            ("2024-01-01T00:00:00Z", "2024-01-01T03:00:00Z"),
            3,
            "2024-01-01T00:00:00Z",
            "2024-01-01T01:00:00Z",
        ),
        # zero-length overall
        ("0 0 * * *", ("2024-01-01", "2024-01-01"), 0, None, None),
        # start > end
        ("0 0 * * *", ("2024-01-05", "2024-01-01"), 0, None, None),
        # monthly — variable-length intervals (28-31 days)
        ("0 0 1 * *", ("2024-01-01", "2024-05-01"), 4, "2024-01-01", "2024-02-01"),
    ],
    ids=["daily", "hourly", "zero-length", "inverted", "monthly-variable"],
)
def test_compute_intervals(
    cron: str,
    overall: tuple,
    expected_count: int,
    first_start: Optional[str],
    first_end: Optional[str],
) -> None:
    iv = _iv(*overall)
    intervals = compute_intervals(cron, iv)
    assert len(intervals) == expected_count
    if first_start is not None:
        assert intervals[0] == (_dt(first_start), _dt(first_end))


def test_monthly_variable_length_intervals() -> None:
    """Monthly cron produces intervals of variable length (28-31 days)."""
    overall = _iv("2024-01-01", "2024-05-01")
    intervals = compute_intervals("0 0 1 * *", overall)
    lengths = [(iv[1] - iv[0]).days for iv in intervals]
    # Jan=31, Feb=29 (2024 leap year), Mar=31, Apr=30
    assert lengths == [31, 29, 31, 30]


def test_weekly_cron() -> None:
    """Weekly cron (every Monday at midnight)."""
    overall = _iv("2024-01-01", "2024-01-29")  # Jan 1 is Monday
    intervals = compute_intervals("0 0 * * 1", overall)
    assert len(intervals) == 4
    for iv in intervals:
        assert (iv[1] - iv[0]).days == 7


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


def test_eligible_intervals_skips_completed() -> None:
    store = IntervalStore()
    store.mark_completed("jobs.a", _dt("2024-01-01"), _dt("2024-01-02"))
    store.mark_completed("jobs.a", _dt("2024-01-02"), _dt("2024-01-03"))
    overall = _iv("2024-01-01", "2024-01-05")
    eligible = get_eligible_intervals("jobs.a", "0 0 * * *", overall, store)
    assert len(eligible) == 2
    assert eligible[0][0] == _dt("2024-01-03")
    store.close()


def test_eligible_intervals_all_when_empty_store() -> None:
    store = IntervalStore()
    overall = _iv("2024-01-01", "2024-01-05")
    eligible = get_eligible_intervals("jobs.a", "0 0 * * *", overall, store)
    assert len(eligible) == 4
    store.close()


def test_eligible_intervals_ordered() -> None:
    store = IntervalStore()
    overall = _iv("2024-01-01", "2024-01-04")
    eligible = get_eligible_intervals("jobs.a", "0 0 * * *", overall, store)
    starts = [iv[0] for iv in eligible]
    assert starts == sorted(starts)
    store.close()


@pytest.mark.parametrize(
    "ds_iv,us_overall,trigger_type,completed,expected",
    [
        # upstream covers downstream interval
        (
            ("2024-01-01", "2024-01-02"),
            ("2024-01-01", "2024-01-31"),
            "job.is_matching_interval_fresh",
            [("2024-01-01", "2024-01-02")],
            True,
        ),
        # upstream has gap for downstream interval
        (
            ("2024-01-01", "2024-01-02"),
            ("2024-01-01", "2024-01-31"),
            "job.is_matching_interval_fresh",
            [],
            False,
        ),
        # downstream before upstream start — assumed fresh
        (
            ("2024-06-01", "2024-06-02"),
            ("2025-01-01", "2025-12-31"),
            "job.is_matching_interval_fresh",
            [],
            True,
        ),
        # is_fresh: upstream fully complete
        (
            ("2024-01-15", "2024-01-16"),
            ("2024-01-01", "2024-01-31"),
            "job.is_fresh",
            [("2024-01-01", "2024-01-31")],
            True,
        ),
        # is_fresh: upstream incomplete
        (
            ("2024-01-15", "2024-01-16"),
            ("2024-01-01", "2024-01-31"),
            "job.is_fresh",
            [("2024-01-01", "2024-01-15")],
            False,
        ),
        # is_fresh: non-overlapping overall → intersection check
        (
            ("2024-06-01", "2024-06-02"),
            ("2025-01-01", "2025-06-01"),
            "job.is_fresh",
            [("2025-01-01", "2025-06-01")],
            True,
        ),
    ],
    ids=[
        "matching-covered",
        "matching-gap",
        "matching-non-overlapping",
        "fresh-complete",
        "fresh-incomplete",
        "fresh-non-overlapping",
    ],
)
def test_check_upstream_freshness(
    ds_iv: tuple,
    us_overall: tuple,
    trigger_type: str,
    completed: List[tuple],
    expected: bool,
) -> None:
    store = IntervalStore()
    for s, e in completed:
        store.mark_completed("jobs.up", _dt(s), _dt(e))
    ds_overall = _iv("2024-01-01", "2025-06-01")
    fresh, _ = check_upstream_freshness(
        _iv(*ds_iv),
        ds_overall,
        "jobs.up",
        _iv(*us_overall),
        trigger_type,
        store,
    )
    assert fresh == expected
    store.close()


def test_all_upstream_must_pass() -> None:
    """One incomplete upstream blocks the downstream."""
    store = IntervalStore()
    store.mark_completed("jobs.a", _dt("2024-01-01"), _dt("2024-01-02"))
    up_a = _job(
        "jobs.a", ["schedule:0 0 * * *"], interval={"start": "2024-01-01", "end": "2024-01-31"}
    )
    up_b = _job(
        "jobs.b", ["schedule:0 0 * * *"], interval={"start": "2024-01-01", "end": "2024-01-31"}
    )
    all_jobs = {"jobs.a": up_a, "jobs.b": up_b}
    freshness = [
        TTrigger("job.is_matching_interval_fresh:jobs.a"),
        TTrigger("job.is_matching_interval_fresh:jobs.b"),
    ]
    fresh, reasons = check_all_upstream_fresh(
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
    store = IntervalStore()
    store.mark_completed("jobs.a", _dt("2024-01-01"), _dt("2024-01-02"))
    store.mark_completed("jobs.b", _dt("2024-01-01"), _dt("2024-01-02"))
    up_a = _job(
        "jobs.a", ["schedule:0 0 * * *"], interval={"start": "2024-01-01", "end": "2024-01-31"}
    )
    up_b = _job(
        "jobs.b", ["schedule:0 0 * * *"], interval={"start": "2024-01-01", "end": "2024-01-31"}
    )
    all_jobs = {"jobs.a": up_a, "jobs.b": up_b}
    freshness = [
        TTrigger("job.is_matching_interval_fresh:jobs.a"),
        TTrigger("job.is_matching_interval_fresh:jobs.b"),
    ]
    fresh, reasons = check_all_upstream_fresh(
        _iv("2024-01-01", "2024-01-02"),
        _iv("2024-01-01", "2024-01-31"),
        freshness,
        all_jobs,
        store,
    )
    assert fresh
    assert reasons == []
    store.close()


def test_iter_intervals_is_lazy() -> None:
    overall = _iv("2024-01-01", "2024-12-31")
    gen = iter_intervals("0 0 * * *", overall)
    first = next(gen)
    assert first == (_dt("2024-01-01"), _dt("2024-01-02"))
    second = next(gen)
    assert second == (_dt("2024-01-02"), _dt("2024-01-03"))


def test_next_eligible_interval_returns_first_incomplete() -> None:
    store = IntervalStore()
    store.mark_completed("jobs.a", _dt("2024-01-01"), _dt("2024-01-02"))
    overall = _iv("2024-01-01", "2024-01-05")
    iv = next_eligible_interval("jobs.a", "0 0 * * *", overall, store)
    assert iv is not None
    assert iv[0] == _dt("2024-01-02")
    store.close()


def test_next_eligible_interval_none_when_all_done() -> None:
    store = IntervalStore()
    store.mark_completed("jobs.a", _dt("2024-01-01"), _dt("2024-01-03"))
    overall = _iv("2024-01-01", "2024-01-03")
    iv = next_eligible_interval("jobs.a", "0 0 * * *", overall, store)
    assert iv is None
    store.close()


def test_daily_covers_hourly_subinterval() -> None:
    store = IntervalStore()
    store.mark_completed("jobs.d", _dt("2024-01-15"), _dt("2024-01-16"))
    assert store.is_range_covered(
        "jobs.d", _dt("2024-01-15T08:00:00Z"), _dt("2024-01-15T09:00:00Z")
    )
    store.close()
