"""Freshness checks that require DuckDBIntervalStore for completion tracking."""

from typing import Dict, List, Optional, Tuple

import pytest

from dlt.common.time import ensure_pendulum_datetime_utc
from dlt.common.typing import TTimeInterval

from dlt._workspace.deployment._interval_store_freshness import (
    check_all_upstream_interval_fresh,
    resolve_interval_freshness_checks,
)
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
from tests.workspace.runner._runner.interval_store import DuckDBIntervalStore


def _iv(start: str, end: str) -> TTimeInterval:
    return (ensure_pendulum_datetime_utc(start), ensure_pendulum_datetime_utc(end))


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


def _check_interval_freshness(
    ds_iv: TTimeInterval,
    ds_overall: TTimeInterval,
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


def _resolve_and_check_interval_freshness(
    ds_iv: TTimeInterval,
    ds_overall: TTimeInterval,
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
        store.mark_interval_completed(
            "jobs.up",
            (ensure_pendulum_datetime_utc(s), ensure_pendulum_datetime_utc(e)),
        )
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


def test_all_upstream_must_pass() -> None:
    """One incomplete upstream blocks the downstream."""
    store = DuckDBIntervalStore()
    store.mark_interval_completed(
        "jobs.a",
        (ensure_pendulum_datetime_utc("2024-01-01"), ensure_pendulum_datetime_utc("2024-01-02")),
    )
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
    store.mark_interval_completed(
        "jobs.a",
        (ensure_pendulum_datetime_utc("2024-01-01"), ensure_pendulum_datetime_utc("2024-01-02")),
    )
    store.mark_interval_completed(
        "jobs.b",
        (ensure_pendulum_datetime_utc("2024-01-01"), ensure_pendulum_datetime_utc("2024-01-02")),
    )
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

    fresh, _ = _check_interval_freshness(
        _iv("2024-01-01T11:40:00Z", "2024-01-01T11:41:00Z"),
        ds_overall,
        up,
        store,
    )
    assert not fresh

    store.mark_interval_completed(
        "jobs.up",
        (
            ensure_pendulum_datetime_utc("2024-01-01T11:39:00Z"),
            ensure_pendulum_datetime_utc("2024-01-01T11:42:00Z"),
        ),
    )
    fresh, _ = _check_interval_freshness(
        _iv("2024-01-01T11:40:00Z", "2024-01-01T11:41:00Z"),
        ds_overall,
        up,
        store,
    )
    assert fresh

    fresh, _ = _check_interval_freshness(
        _iv("2024-01-01T11:42:00Z", "2024-01-01T11:43:00Z"),
        ds_overall,
        up,
        store,
    )
    assert not fresh

    store.mark_interval_completed(
        "jobs.up",
        (
            ensure_pendulum_datetime_utc("2024-01-01T11:42:00Z"),
            ensure_pendulum_datetime_utc("2024-01-01T11:45:00Z"),
        ),
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
        "jobs.up",
        (
            ensure_pendulum_datetime_utc("2024-01-01T11:39:00Z"),
            ensure_pendulum_datetime_utc("2024-01-01T11:42:00Z"),
        ),
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
