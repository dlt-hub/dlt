"""Tests for runner refresh-signal wiring (`_on_non_interval_success` and friends)."""

from typing import Any, Dict, Iterator, List, Optional

import pytest

from dlt.common.pendulum import pendulum
from dlt.common.time import ensure_pendulum_datetime_utc

from dlt._workspace._runner import runner as runner_mod
from dlt._workspace._runner.freshness_store import DuckDBJobFreshnessStore
from dlt._workspace._runner.run_store import TJobRun
from dlt._workspace.deployment.typing import (
    TEntryPoint,
    TExecuteSpec,
    TFreshnessConstraint,
    TJobDefinition,
    TJobRef,
    TRefreshPolicy,
    TTrigger,
)


def _dt(s: str) -> pendulum.DateTime:
    return ensure_pendulum_datetime_utc(s)


def _job(
    ref: str,
    refresh: Optional[TRefreshPolicy] = None,
    freshness: Optional[List[str]] = None,
) -> TJobDefinition:
    job: TJobDefinition = {
        "job_ref": TJobRef(ref),
        "entry_point": TEntryPoint(module="m", function="f", job_type="batch"),
        "triggers": [TTrigger(f"manual:{ref}")],
        "execute": TExecuteSpec(),
    }
    if refresh is not None:
        job["refresh"] = refresh
    if freshness is not None:
        job["freshness"] = [TFreshnessConstraint(c) for c in freshness]
    return job


def _run_record(
    job_ref: str,
    started_at: pendulum.DateTime,
    interval_start: Optional[pendulum.DateTime] = None,
    interval_end: Optional[pendulum.DateTime] = None,
) -> TJobRun:
    rec: TJobRun = {
        "run_id": "rid",
        "job_ref": job_ref,
        "trigger": f"manual:{job_ref}",
        "scheduled_at": started_at,
        "started_at": started_at,
        "status": "completed",
    }
    if interval_start is not None:
        rec["interval_start"] = interval_start
    if interval_end is not None:
        rec["interval_end"] = interval_end
    return rec


@pytest.fixture
def runner_state() -> Iterator[Dict[str, TJobDefinition]]:
    """Set up module-level runner state for unit tests; restore on teardown."""
    from dlt._workspace._runner.run_store import DuckDBJobRunsStore

    saved_freshness = runner_mod._freshness_store
    saved_runs = runner_mod._runs_store
    saved_jobs = runner_mod._all_jobs_map
    saved_processes = runner_mod._processes
    saved_run_ids = runner_mod._running_run_ids
    saved_refresh_flags = runner_mod._running_refresh_flags
    runner_mod._freshness_store = DuckDBJobFreshnessStore()
    runner_mod._runs_store = DuckDBJobRunsStore()
    runner_mod._all_jobs_map = {}
    runner_mod._processes = {}
    runner_mod._running_run_ids = {}
    runner_mod._running_refresh_flags = {}
    try:
        yield runner_mod._all_jobs_map
    finally:
        runner_mod._freshness_store.close()
        runner_mod._runs_store.close()
        runner_mod._freshness_store = saved_freshness
        runner_mod._runs_store = saved_runs
        runner_mod._all_jobs_map = saved_jobs
        runner_mod._processes = saved_processes
        runner_mod._running_run_ids = saved_run_ids
        runner_mod._running_refresh_flags = saved_refresh_flags


# _on_non_interval_success — prev_completed_run update


def test_prev_completed_run_set_from_started_at(
    runner_state: Dict[str, TJobDefinition],
) -> None:
    """`started_at` becomes the new `prev_completed_run` regardless of `interval_start`.

    Using `interval_start` would freeze `prev_completed_run` at the first cron tick
    forever (since the next run's interval_start is computed from prev itself, so
    interval_start == prev). `started_at` advances on every run, which is what we want.
    """
    runner_state["jobs.a"] = _job("jobs.a")
    started_at = _dt("2024-06-15T12:30:00Z")
    iv_start = _dt("2024-06-15T12:00:00Z")
    record = _run_record("jobs.a", started_at, interval_start=iv_start)
    runner_mod._on_non_interval_success("jobs.a", runner_state["jobs.a"], record, False)
    assert runner_mod._freshness_store.get_prev_completed_run("jobs.a") == started_at


def test_prev_completed_run_advances_across_consecutive_runs(
    runner_state: Dict[str, TJobDefinition],
) -> None:
    """Regression: with a `schedule:*/2 * * * *` job, prev advances on each run.

    The original bug used `interval_start` for the new prev — and since
    `compute_run_interval(prev_set, now)` returns `(prev, now)`, every subsequent
    run's interval_start equaled the previous prev, freezing the watermark at the
    first cron tick. This made downstream freshness checks fail because the
    upstream's `prev_completed_run` never moved past the first run's tick.
    """
    runner_state["jobs.a"] = _job("jobs.a")

    # simulate run 1: started_at in the 23:38 minute, interval_start at the cron tick
    run1_started = _dt("2026-04-08T23:38:30Z")
    rec1 = _run_record(
        "jobs.a",
        run1_started,
        interval_start=_dt("2026-04-08T23:38:00Z"),
    )
    runner_mod._on_non_interval_success("jobs.a", runner_state["jobs.a"], rec1, True)
    assert runner_mod._freshness_store.get_prev_completed_run("jobs.a") == run1_started

    # simulate run 2: dispatched at 23:40:30. Its interval_start would be the
    # previous prev (23:38:30), but prev should now advance to run 2's started_at.
    run2_started = _dt("2026-04-08T23:40:30Z")
    rec2 = _run_record(
        "jobs.a",
        run2_started,
        interval_start=run1_started,  # this is what compute_run_interval would produce
    )
    runner_mod._on_non_interval_success("jobs.a", runner_state["jobs.a"], rec2, False)
    assert runner_mod._freshness_store.get_prev_completed_run("jobs.a") == run2_started

    # simulate run 3 — same pattern
    run3_started = _dt("2026-04-08T23:42:30Z")
    rec3 = _run_record(
        "jobs.a",
        run3_started,
        interval_start=run2_started,
    )
    runner_mod._on_non_interval_success("jobs.a", runner_state["jobs.a"], rec3, False)
    assert runner_mod._freshness_store.get_prev_completed_run("jobs.a") == run3_started


def test_no_run_record_is_noop(runner_state: Dict[str, TJobDefinition]) -> None:
    """Missing run record leaves the freshness store untouched."""
    runner_state["jobs.a"] = _job("jobs.a")
    runner_mod._on_non_interval_success("jobs.a", runner_state["jobs.a"], None, False)
    assert runner_mod._freshness_store.get_prev_completed_run("jobs.a") is None


# _on_non_interval_success — policy propagation to direct downstream


@pytest.mark.parametrize(
    "policy,this_run_was_refresh,should_propagate",
    [
        # auto: propagate only when this run had refresh=True
        ("auto", True, True),
        ("auto", False, False),
        # always: propagate regardless of input refresh signal
        ("always", True, True),
        ("always", False, True),
        # block: never propagate
        ("block", True, False),
        ("block", False, False),
    ],
    ids=[
        "auto-with-refresh",
        "auto-without-refresh",
        "always-with-refresh",
        "always-without-refresh",
        "block-with-refresh",
        "block-without-refresh",
    ],
)
def test_policy_propagation(
    runner_state: Dict[str, TJobDefinition],
    policy: TRefreshPolicy,
    this_run_was_refresh: bool,
    should_propagate: bool,
) -> None:
    """Refresh policy decides whether to clear direct downstream prev_completed_run."""
    runner_state["jobs.a"] = _job("jobs.a", refresh=policy)
    runner_state["jobs.b"] = _job("jobs.b", freshness=["job.is_fresh:jobs.a"])
    # seed downstream with a non-None value so we can detect a clear
    runner_mod._freshness_store.set_prev_completed_run("jobs.b", _dt("2024-06-01T00:00:00Z"))

    record = _run_record("jobs.a", _dt("2024-06-15T12:00:00Z"))
    runner_mod._on_non_interval_success(
        "jobs.a", runner_state["jobs.a"], record, this_run_was_refresh
    )

    if should_propagate:
        assert runner_mod._freshness_store.get_prev_completed_run("jobs.b") is None
    else:
        assert runner_mod._freshness_store.get_prev_completed_run("jobs.b") == _dt(
            "2024-06-01T00:00:00Z"
        )


def test_default_policy_is_auto(runner_state: Dict[str, TJobDefinition]) -> None:
    """Job without explicit refresh policy is treated as auto."""
    runner_state["jobs.a"] = _job("jobs.a")  # no refresh field → default
    runner_state["jobs.b"] = _job("jobs.b", freshness=["job.is_fresh:jobs.a"])
    runner_mod._freshness_store.set_prev_completed_run("jobs.b", _dt("2024-06-01T00:00:00Z"))

    record = _run_record("jobs.a", _dt("2024-06-15T12:00:00Z"))
    # this run had refresh=True → auto should propagate
    runner_mod._on_non_interval_success("jobs.a", runner_state["jobs.a"], record, True)
    assert runner_mod._freshness_store.get_prev_completed_run("jobs.b") is None


def test_propagation_only_to_direct_downstream(
    runner_state: Dict[str, TJobDefinition],
) -> None:
    """Policy only clears DIRECT downstream — transitive nodes are not touched here."""
    runner_state["jobs.a"] = _job("jobs.a", refresh="always")
    runner_state["jobs.b"] = _job("jobs.b", freshness=["job.is_fresh:jobs.a"])
    runner_state["jobs.c"] = _job("jobs.c", freshness=["job.is_fresh:jobs.b"])
    runner_mod._freshness_store.set_prev_completed_run("jobs.b", _dt("2024-06-01T00:00:00Z"))
    runner_mod._freshness_store.set_prev_completed_run("jobs.c", _dt("2024-06-01T00:00:00Z"))

    record = _run_record("jobs.a", _dt("2024-06-15T12:00:00Z"))
    runner_mod._on_non_interval_success("jobs.a", runner_state["jobs.a"], record, False)

    # b is direct downstream → cleared
    assert runner_mod._freshness_store.get_prev_completed_run("jobs.b") is None
    # c is transitive → untouched (will be cleared when b runs and applies its own policy)
    assert runner_mod._freshness_store.get_prev_completed_run("jobs.c") == _dt(
        "2024-06-01T00:00:00Z"
    )


# _eager_refresh_cascade — --refresh CLI cascade


def _interval_job(ref: str) -> TJobDefinition:
    """An interval-store-eligible job (interval + allow_external_schedulers)."""
    job = _job(ref)
    job["interval"] = {"start": "2024-01-01T00:00:00Z"}
    job["allow_external_schedulers"] = True
    job["triggers"] = [TTrigger("schedule:0 * * * *")]
    return job


def test_cascade_clears_target_and_transitive_downstream(
    runner_state: Dict[str, TJobDefinition],
) -> None:
    """--refresh on the seed job clears prev_completed_run for the seed and all transitive downstream."""
    runner_state["jobs.a"] = _job("jobs.a")
    runner_state["jobs.b"] = _job("jobs.b", freshness=["job.is_fresh:jobs.a"])
    runner_state["jobs.c"] = _job("jobs.c", freshness=["job.is_fresh:jobs.b"])
    runner_state["jobs.d"] = _job("jobs.d", freshness=["job.is_fresh:jobs.c"])
    seed_ts = _dt("2024-06-01T00:00:00Z")
    for ref in ("jobs.a", "jobs.b", "jobs.c", "jobs.d"):
        runner_mod._freshness_store.set_prev_completed_run(ref, seed_ts)

    warnings: List[str] = []
    runner_mod._eager_refresh_cascade(
        [(runner_state["jobs.a"], TTrigger("manual:jobs.a"))],
        warn=warnings.append,
    )

    assert warnings == []
    for ref in ("jobs.a", "jobs.b", "jobs.c", "jobs.d"):
        assert runner_mod._freshness_store.get_prev_completed_run(ref) is None, ref


def test_cascade_skips_interval_store_jobs_in_downstream(
    runner_state: Dict[str, TJobDefinition],
) -> None:
    """Interval-store jobs in the downstream subgraph are silently excluded from clearing."""
    runner_state["jobs.a"] = _job("jobs.a")
    # b is an interval-store job (has interval + allow_external_schedulers)
    runner_state["jobs.b"] = _interval_job("jobs.b")
    runner_state["jobs.b"]["freshness"] = [TFreshnessConstraint("job.is_fresh:jobs.a")]
    runner_state["jobs.c"] = _job("jobs.c", freshness=["job.is_fresh:jobs.b"])
    runner_mod._freshness_store.set_prev_completed_run("jobs.a", _dt("2024-06-01T00:00:00Z"))
    runner_mod._freshness_store.set_prev_completed_run("jobs.b", _dt("2024-06-01T00:00:00Z"))
    runner_mod._freshness_store.set_prev_completed_run("jobs.c", _dt("2024-06-01T00:00:00Z"))

    runner_mod._eager_refresh_cascade(
        [(runner_state["jobs.a"], TTrigger("manual:jobs.a"))],
        warn=lambda _m: None,
    )

    # a is non-interval → cleared
    assert runner_mod._freshness_store.get_prev_completed_run("jobs.a") is None
    # b is interval-store → NOT cleared (not in scope of the new feature)
    assert runner_mod._freshness_store.get_prev_completed_run("jobs.b") == _dt(
        "2024-06-01T00:00:00Z"
    )
    # c is non-interval transitive downstream → still cleared
    assert runner_mod._freshness_store.get_prev_completed_run("jobs.c") is None


def test_cascade_skips_interval_store_seed(
    runner_state: Dict[str, TJobDefinition],
) -> None:
    """Interval-store jobs as the seed are silently skipped (no cascade, no warning)."""
    runner_state["jobs.a"] = _interval_job("jobs.a")
    runner_state["jobs.b"] = _job("jobs.b", freshness=["job.is_fresh:jobs.a"])
    runner_mod._freshness_store.set_prev_completed_run("jobs.b", _dt("2024-06-01T00:00:00Z"))

    warnings: List[str] = []
    runner_mod._eager_refresh_cascade(
        [(runner_state["jobs.a"], TTrigger("schedule:0 * * * *"))],
        warn=warnings.append,
    )

    assert warnings == []
    # b should be untouched because the seed (interval-store) is silently skipped
    assert runner_mod._freshness_store.get_prev_completed_run("jobs.b") == _dt(
        "2024-06-01T00:00:00Z"
    )


@pytest.mark.parametrize(
    "manifest_value,expected_entry_point_value",
    [
        (None, False),  # not set in manifest → entry_point gets False
        (False, False),
        (True, True),
    ],
    ids=["unset", "explicit-false", "explicit-true"],
)
def test_allow_external_schedulers_propagates_to_entry_point(
    runner_state: Dict[str, TJobDefinition],
    manifest_value: Any,
    expected_entry_point_value: bool,
) -> None:
    """`_start_job` propagates `job_def["allow_external_schedulers"]` into the entry_point."""
    job_def = _job("jobs.a")
    if manifest_value is not None:
        job_def["allow_external_schedulers"] = manifest_value
    runner_state["jobs.a"] = job_def

    # call _start_job's entry_point construction logic by stubbing JobProcess so
    # we don't actually fork. We capture the entry_point JSON via the cmd argument.
    captured_cmds: List[List[str]] = []

    class _FakeProc:
        DEFAULT_GRACE_PERIOD = 30.0
        is_alive_value = True

        def __init__(self, job_ref: str, cmd: List[str], grace_period: float = 30.0) -> None:
            self.job_ref = job_ref
            self.cmd = cmd
            captured_cmds.append(cmd)

        def start(self) -> None:
            pass

        def is_alive(self) -> bool:
            return False

    import dlt._workspace._runner.runner as runner_mod_inner

    saved_cls = runner_mod_inner.JobProcess
    runner_mod_inner.JobProcess = _FakeProc  # type: ignore[misc,assignment]
    try:
        runner_mod._start_job(job_def, TTrigger("manual:jobs.a"), port_counter=[8000])
    finally:
        runner_mod_inner.JobProcess = saved_cls  # type: ignore[misc]

    assert captured_cmds, "expected _start_job to spawn a (fake) process"
    # entry_point JSON is the value following "--entry-point" in the cmd
    cmd = captured_cmds[0]
    ep_idx = cmd.index("--entry-point") + 1
    import json as _json

    ep = _json.loads(cmd[ep_idx])
    assert ep.get("allow_external_schedulers", "missing") == expected_entry_point_value
    # interval should be set since this is a non-interval job dispatched manually
    assert "interval_start" in ep
    assert "interval_end" in ep


def test_cascade_skipped_when_seed_blocked_by_freshness(
    runner_state: Dict[str, TJobDefinition],
) -> None:
    """Seed whose own freshness check would fail is skipped with a warning, no clearing."""
    # b has freshness on a, but a has prev_completed_run = None → b's freshness fails
    runner_state["jobs.a"] = _job("jobs.a")
    runner_state["jobs.b"] = _job("jobs.b", freshness=["job.is_fresh:jobs.a"])
    runner_state["jobs.c"] = _job("jobs.c", freshness=["job.is_fresh:jobs.b"])
    # a has no prev_completed_run → b's pre-flight check fails
    runner_mod._freshness_store.set_prev_completed_run("jobs.b", _dt("2024-06-01T00:00:00Z"))
    runner_mod._freshness_store.set_prev_completed_run("jobs.c", _dt("2024-06-01T00:00:00Z"))

    warnings: List[str] = []
    runner_mod._eager_refresh_cascade(
        [(runner_state["jobs.b"], TTrigger("manual:jobs.b"))],
        warn=warnings.append,
    )

    assert len(warnings) == 1
    assert "skipped" in warnings[0]
    # b and c are NOT cleared because the seed pre-flight failed
    assert runner_mod._freshness_store.get_prev_completed_run("jobs.b") == _dt(
        "2024-06-01T00:00:00Z"
    )
    assert runner_mod._freshness_store.get_prev_completed_run("jobs.c") == _dt(
        "2024-06-01T00:00:00Z"
    )
