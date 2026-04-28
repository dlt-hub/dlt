"""Tests for run-based upstream freshness checks (`check_all_upstream_run_fresh`)."""

from datetime import datetime, timedelta, timezone  # noqa: I251
from typing import Dict, List, Optional, Tuple

import pytest

from dlt._workspace.deployment.interval import check_all_upstream_run_fresh
from dlt._workspace.deployment.typing import (
    TEntryPoint,
    TExecuteSpec,
    TFreshnessConstraint,
    TIntervalSpec,
    TJobDefinition,
    TJobRef,
    TJobType,
    TRefreshPolicy,
    TTrigger,
)


def _job(
    ref: str,
    triggers: Optional[List[str]] = None,
    interval: Optional[TIntervalSpec] = None,
    default_trigger: Optional[str] = None,
    job_type: TJobType = "batch",
    freshness: Optional[List[str]] = None,
    refresh: Optional[TRefreshPolicy] = None,
    allow_external_schedulers: Optional[bool] = None,
) -> TJobDefinition:
    job: TJobDefinition = {
        "job_ref": TJobRef(ref),
        "entry_point": TEntryPoint(
            module="m",
            function="f",
            job_type=job_type,
            launcher="dlt._workspace.deployment.launchers.job",
        ),
        "triggers": [TTrigger(t) for t in (triggers or [])],
        "execute": TExecuteSpec(),
    }
    if interval is not None:
        job["interval"] = interval
    if default_trigger is not None:
        job["default_trigger"] = TTrigger(default_trigger)
    if freshness is not None:
        job["freshness"] = [TFreshnessConstraint(c) for c in freshness]
    if refresh is not None:
        job["refresh"] = refresh
    if allow_external_schedulers is not None:
        job["allow_external_schedulers"] = allow_external_schedulers
    return job


def _check_run_freshness(
    upstream_job: TJobDefinition,
    prev_interval_end: Optional[datetime],
    now_utc: Optional[datetime] = None,
    constraint: str = "job.is_fresh",
) -> Tuple[bool, List[str]]:
    """Run check_all_upstream_run_fresh with a single upstream."""
    ref = upstream_job["job_ref"]
    all_jobs: Dict[str, TJobDefinition] = {ref: upstream_job}
    freshness = [TFreshnessConstraint(f"{constraint}:{ref}")]
    return check_all_upstream_run_fresh(
        freshness, all_jobs, {ref: prev_interval_end}, now_utc=now_utc
    )


# fixed reference sitting exactly on an hour boundary so schedule boundary cases are deterministic
_BOUNDARY = datetime(2026, 4, 22, 12, 0, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    "trigger,now_utc,prev_interval_end,expected_fresh,reason_frag",
    [
        # schedule:0 * * * * — hourly ticks at the top of the hour
        # mid-interval: prev_interval_end covers the current tick → fresh
        ("schedule:0 * * * *", _BOUNDARY + timedelta(minutes=30), _BOUNDARY, True, None),
        # utc_now exactly at interval_end (the tick itself) and prev_interval_end == tick → fresh
        ("schedule:0 * * * *", _BOUNDARY, _BOUNDARY, True, None),
        # utc_now exactly on the tick, prev_interval_end at the previous tick
        # (one full interval behind) → stale (interval_end belongs to the next interval —
        # analog of the every elapsed==period case)
        (
            "schedule:0 * * * *",
            _BOUNDARY,
            _BOUNDARY - timedelta(hours=1),
            False,
            "missing run",
        ),
        # 5s before the next tick: expected tick is the previous hour;
        # prev_interval_end at that tick → fresh
        (
            "schedule:0 * * * *",
            _BOUNDARY - timedelta(seconds=5),
            _BOUNDARY - timedelta(hours=1),
            True,
            None,
        ),
        # 5s before the next tick but prev_interval_end is two ticks back → stale
        (
            "schedule:0 * * * *",
            _BOUNDARY - timedelta(seconds=5),
            _BOUNDARY - timedelta(hours=2),
            False,
            "missing run",
        ),
        # 5s after a tick: expected tick is the just-elapsed one;
        # prev_interval_end at that tick → fresh
        ("schedule:0 * * * *", _BOUNDARY + timedelta(seconds=5), _BOUNDARY, True, None),
        # 5s after a tick and prev_interval_end is the previous hour → stale (missed the new tick)
        (
            "schedule:0 * * * *",
            _BOUNDARY + timedelta(seconds=5),
            _BOUNDARY - timedelta(hours=1),
            False,
            "missing run",
        ),
        # mid-interval, prev_interval_end two ticks behind → stale
        (
            "schedule:0 * * * *",
            _BOUNDARY + timedelta(minutes=30),
            _BOUNDARY - timedelta(hours=2),
            False,
            "missing run",
        ),
        # no prev_interval_end → not in usable state
        (
            "schedule:0 * * * *",
            _BOUNDARY + timedelta(minutes=30),
            None,
            False,
            "didn't process any intervals",
        ),
        # every:5m — prev_interval_end 2m ago → fresh (within period)
        ("every:5m", _BOUNDARY, _BOUNDARY - timedelta(minutes=2), True, None),
        # every:5m — elapsed one second below period → fresh
        (
            "every:5m",
            _BOUNDARY,
            _BOUNDARY - timedelta(minutes=4, seconds=59),
            True,
            None,
        ),
        # every:5m — elapsed exactly period_seconds → stale (interval_end belongs to next period)
        ("every:5m", _BOUNDARY, _BOUNDARY - timedelta(minutes=5), False, "missing run"),
        # every:5m — one extra second past the period → stale
        (
            "every:5m",
            _BOUNDARY,
            _BOUNDARY - timedelta(minutes=5, seconds=1),
            False,
            "missing run",
        ),
        # every:5m — prev_interval_end 12m ago → stale
        ("every:5m", _BOUNDARY, _BOUNDARY - timedelta(minutes=12), False, "missing run"),
        # every:5m — no prev_interval_end → not in usable state
        ("every:5m", _BOUNDARY, None, False, "didn't process any intervals"),
        # event — prev_interval_end set → fresh
        (
            "job.success:jobs.other",
            _BOUNDARY,
            _BOUNDARY - timedelta(minutes=2),
            True,
            None,
        ),
        # event — no prev_interval_end → not in usable state
        ("job.success:jobs.other", _BOUNDARY, None, False, "didn't process any intervals"),
    ],
    ids=[
        "schedule-mid-interval-fresh",
        "schedule-now-exactly-at-interval-end-fresh",
        "schedule-now-exactly-at-interval-end-prev-one-tick-behind-stale",
        "schedule-now-5s-below-boundary-fresh",
        "schedule-now-5s-below-boundary-stale",
        "schedule-now-5s-above-boundary-fresh",
        "schedule-now-5s-above-boundary-stale",
        "schedule-two-ticks-behind",
        "schedule-no-prev",
        "every-recent-fresh",
        "every-elapsed-just-below-period-fresh",
        "every-elapsed-exactly-period-stale",
        "every-elapsed-just-above-period-stale",
        "every-two-periods-old",
        "every-no-prev",
        "event-prev-set",
        "event-no-prev",
    ],
)
def test_run_based_freshness(
    trigger: str,
    now_utc: datetime,
    prev_interval_end: Optional[datetime],
    expected_fresh: bool,
    reason_frag: Optional[str],
) -> None:
    """Parametrized test for run-based freshness: schedule-no-interval, every, event."""
    up = _job("jobs.up", [trigger], default_trigger=trigger)
    fresh, reasons = _check_run_freshness(up, prev_interval_end, now_utc=now_utc)

    assert fresh == expected_fresh
    if reason_frag:
        assert any(reason_frag in r for r in reasons), f"expected '{reason_frag}' in {reasons}"


def test_freshness_interactive_upstream_always_not_fresh() -> None:
    """Interactive upstream cannot be fresh."""
    up = _job("jobs.up", ["http:"], default_trigger="http:", job_type="interactive")
    fresh, reasons = _check_run_freshness(up, None)
    assert not fresh
    assert "interactive" in reasons[0]


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


@pytest.mark.parametrize(
    "trigger,default_trigger,job_type_,constraint,has_prev_interval_end,reason_frag",
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
        # no default_trigger, no prev_interval_end → event path, stale
        ("manual:jobs.up", None, "batch", "job.is_fresh", False, "didn't process any intervals"),
        # no default_trigger, prev_interval_end set → event path, fresh
        ("manual:jobs.up", None, "batch", "job.is_fresh", True, None),
    ],
    ids=[
        "rejects-is-matching-interval-fresh",
        "rejects-interactive",
        "no-trigger-no-prev-interval-end",
        "no-trigger-with-prev-interval-end",
    ],
)
def test_run_fresh_edge_cases(
    trigger: str,
    default_trigger: Optional[str],
    job_type_: str,
    constraint: str,
    has_prev_interval_end: bool,
    reason_frag: Optional[str],
) -> None:
    """Parametrized edge cases for check_all_upstream_run_fresh."""
    prev_interval_end: Optional[datetime] = _BOUNDARY if has_prev_interval_end else None
    up = _job("jobs.up", [trigger], default_trigger=default_trigger, job_type=job_type_)  # type: ignore[arg-type]
    fresh, reasons = _check_run_freshness(
        up, prev_interval_end, now_utc=_BOUNDARY, constraint=constraint
    )
    if reason_frag is None:
        assert fresh, f"should be fresh: {reasons}"
    else:
        assert not fresh
        assert reason_frag in reasons[0]


def test_run_fresh_mixed_upstreams() -> None:
    """Multiple constraints: one passes, one fails."""
    up_a = _job("jobs.a", ["every:5m"], default_trigger="every:5m")
    up_b = _job("jobs.b", ["every:5m"], default_trigger="every:5m")
    all_jobs: Dict[str, TJobDefinition] = {"jobs.a": up_a, "jobs.b": up_b}
    freshness = [
        TFreshnessConstraint("job.is_fresh:jobs.a"),
        TFreshnessConstraint("job.is_fresh:jobs.b"),
    ]
    prev_interval_ends: Dict[str, Optional[datetime]] = {
        "jobs.a": _BOUNDARY,
        "jobs.b": None,
    }
    fresh, reasons = check_all_upstream_run_fresh(
        freshness, all_jobs, prev_interval_ends, now_utc=_BOUNDARY
    )
    assert not fresh
    assert len(reasons) == 1
    assert "jobs.b" in reasons[0]
