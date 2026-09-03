"""Tests for runner refresh-signal wiring (`_on_non_interval_success` and friends)."""

from typing import Any, Callable, Dict, Iterator, List, Optional, Set

import pytest

from datetime import datetime

from dlt.common.time import ensure_datetime_in_tz

from tests.workspace.runner._runner import runner as runner_mod
from tests.workspace.runner._runner.freshness_store import DuckDBJobFreshnessStore
from tests.workspace.runner._runner.run_store import TJobRun
from dlt._workspace.deployment.typing import (
    TEntryPoint,
    TExecuteSpec,
    TFreshnessConstraint,
    TJobDefinition,
    TJobRef,
    TRefreshPolicy,
    TTrigger,
)

from tests.workspace.manifest_utils import make_job, make_manifest


def _dt(s: str) -> datetime:
    return ensure_datetime_in_tz(s)


def _job(
    ref: str,
    freshness: Optional[List[str]] = None,
    refresh_propagation: Optional[TRefreshPolicy] = None,
) -> TJobDefinition:
    optional: Dict[str, Any] = {
        "freshness": freshness,
        "refresh_propagation": refresh_propagation,
    }
    return make_job(
        ref,
        module="m",
        function="f",
        triggers=[f"manual:{ref}"],
        concurrency=None,
        **{k: v for k, v in optional.items() if v is not None},
    )


def _run_record(
    job_ref: str,
    started_at: datetime,
    interval_start: Optional[datetime] = None,
    interval_end: Optional[datetime] = None,
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
    from tests.workspace.runner._runner.run_store import DuckDBJobRunsStore

    saved_freshness = runner_mod._freshness_store
    saved_runs = runner_mod._runs_store
    saved_jobs = runner_mod._all_jobs_map
    saved_processes = runner_mod._processes
    saved_run_ids = runner_mod._running_run_ids
    runner_mod._freshness_store = DuckDBJobFreshnessStore()
    runner_mod._runs_store = DuckDBJobRunsStore()
    runner_mod._all_jobs_map = {}
    runner_mod._processes = {}
    runner_mod._running_run_ids = {}
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
    runner_mod._on_non_interval_success("jobs.a", record)
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
    runner_mod._on_non_interval_success("jobs.a", rec1)
    assert runner_mod._freshness_store.get_prev_completed_run("jobs.a") == run1_started

    # simulate run 2: dispatched at 23:40:30. Its interval_start would be the
    # previous prev (23:38:30), but prev should now advance to run 2's started_at.
    run2_started = _dt("2026-04-08T23:40:30Z")
    rec2 = _run_record(
        "jobs.a",
        run2_started,
        interval_start=run1_started,  # this is what compute_run_interval would produce
    )
    runner_mod._on_non_interval_success("jobs.a", rec2)
    assert runner_mod._freshness_store.get_prev_completed_run("jobs.a") == run2_started

    # simulate run 3 — same pattern
    run3_started = _dt("2026-04-08T23:42:30Z")
    rec3 = _run_record(
        "jobs.a",
        run3_started,
        interval_start=run2_started,
    )
    runner_mod._on_non_interval_success("jobs.a", rec3)
    assert runner_mod._freshness_store.get_prev_completed_run("jobs.a") == run3_started


def test_no_run_record_is_noop(runner_state: Dict[str, TJobDefinition]) -> None:
    """Missing run record leaves the freshness store untouched."""
    runner_state["jobs.a"] = _job("jobs.a")
    runner_mod._on_non_interval_success("jobs.a", None)
    assert runner_mod._freshness_store.get_prev_completed_run("jobs.a") is None


def test_set_prev_completed_run_is_monotonic(
    runner_state: Dict[str, TJobDefinition],
) -> None:
    """Out-of-order completions cannot move the watermark backward.

    Two concurrent runs of the same script can complete out of order
    (e.g. an older interactive instance finishing after a newer one).
    Only the strictly-greater value should win.
    """
    runner_state["jobs.a"] = _job("jobs.a")
    later = _dt("2026-04-08T23:42:00Z")
    earlier = _dt("2026-04-08T23:38:00Z")

    # the later watermark wins even though it's set first
    runner_mod._freshness_store.set_prev_completed_run("jobs.a", later)
    runner_mod._freshness_store.set_prev_completed_run("jobs.a", earlier)
    assert runner_mod._freshness_store.get_prev_completed_run("jobs.a") == later

    # explicit clears bypass the monotonic guard
    runner_mod._freshness_store.clear_prev_completed_run("jobs.a")
    assert runner_mod._freshness_store.get_prev_completed_run("jobs.a") is None
    runner_mod._freshness_store.set_prev_completed_run("jobs.a", earlier)
    assert runner_mod._freshness_store.get_prev_completed_run("jobs.a") == earlier


def _interval_job(ref: str) -> TJobDefinition:
    """An interval-store-eligible job (parallel interval mode)."""
    job = _job(ref)
    job["interval"] = {"start": "2024-01-01T00:00:00Z", "mode": "parallel"}
    job["incremental_mode"] = "interval"
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


def test_cascade_walks_through_interval_store_jobs_in_downstream(
    runner_state: Dict[str, TJobDefinition],
) -> None:
    """The cascade walks through interval-store jobs in the downstream.

    Severing at interval-store jobs is not implemented on the runtime.
    """
    runner_state["jobs.a"] = _job("jobs.a")
    # b is an interval-store job (parallel interval mode)
    runner_state["jobs.b"] = _interval_job("jobs.b")
    runner_state["jobs.b"]["freshness"] = [TFreshnessConstraint("job.is_fresh:jobs.a")]
    runner_state["jobs.c"] = _job("jobs.c", freshness=["job.is_fresh:jobs.b"])
    seed_ts = _dt("2024-06-01T00:00:00Z")
    runner_mod._freshness_store.set_prev_completed_run("jobs.a", seed_ts)
    runner_mod._freshness_store.set_prev_completed_run("jobs.b", seed_ts)
    runner_mod._freshness_store.set_prev_completed_run("jobs.c", seed_ts)

    runner_mod._eager_refresh_cascade(
        [(runner_state["jobs.a"], TTrigger("manual:jobs.a"))],
        warn=lambda _m: None,
    )

    # the whole downstream is cleared, including the interval-store job
    for ref in ("jobs.a", "jobs.b", "jobs.c"):
        assert runner_mod._freshness_store.get_prev_completed_run(ref) is None, ref


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
    "jd_patch,expected_allow,expected_mode",
    [
        ({}, None, None),  # not set in manifest → entry_point gets neither key
        ({"incremental_mode": "interval"}, True, "interval"),
        ({"incremental_mode": "pipeline"}, False, "pipeline"),
        ({"auto_refresh_pipeline_mode": "drop_sources"}, None, None),
    ],
    ids=["unset", "mode-interval", "mode-pipeline", "auto-refresh-mode"],
)
def test_incremental_mode_propagates_to_entry_point(
    runner_state: Dict[str, TJobDefinition],
    jd_patch: Dict[str, Any],
    expected_allow: bool,
    expected_mode: Any,
) -> None:
    """`_start_job` propagates incremental mode into the entry_point."""
    job_def = _job("jobs.a")
    job_def.update(jd_patch)  # type: ignore[typeddict-item]
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

    import tests.workspace.runner._runner.runner as runner_mod_inner

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
    assert ep.get("allow_external_schedulers") is expected_allow
    assert ep.get("incremental_mode") == expected_mode
    # auto_refresh_pipeline_mode passes through verbatim when present
    assert ep.get("auto_refresh_pipeline_mode") == jd_patch.get("auto_refresh_pipeline_mode")
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


@pytest.fixture
def stubbed_job_process() -> Iterator[List[List[str]]]:
    """Replace `JobProcess` with a fake that captures cmd args without forking."""
    captured_cmds: List[List[str]] = []

    class _FakeProc:
        DEFAULT_GRACE_PERIOD = 30.0

        def __init__(self, job_ref: str, cmd: List[str], grace_period: float = 30.0) -> None:
            self.job_ref = job_ref
            self.cmd = cmd
            captured_cmds.append(cmd)

        def start(self) -> None:
            pass

        def is_alive(self) -> bool:
            return False

    saved_cls = runner_mod.JobProcess
    runner_mod.JobProcess = _FakeProc  # type: ignore[misc,assignment]
    try:
        yield captured_cmds
    finally:
        runner_mod.JobProcess = saved_cls  # type: ignore[misc]


def _block_root_graph() -> Dict[str, TJobDefinition]:
    return {
        "jobs.a": _job("jobs.a", refresh_propagation="block"),
        "jobs.b": _job("jobs.b", freshness=["job.is_fresh:jobs.a"]),
    }


def _block_in_chain_graph() -> Dict[str, TJobDefinition]:
    return {
        "jobs.a": _job("jobs.a"),
        "jobs.b": _job("jobs.b", freshness=["job.is_fresh:jobs.a"]),
        "jobs.c": _job("jobs.c", freshness=["job.is_fresh:jobs.b"], refresh_propagation="block"),
        "jobs.d": _job("jobs.d", freshness=["job.is_fresh:jobs.c"]),
    }


def _block_in_diamond_graph() -> Dict[str, TJobDefinition]:
    return {
        "jobs.a": _job("jobs.a"),
        "jobs.b": _job("jobs.b", freshness=["job.is_fresh:jobs.a"]),
        "jobs.c": _job("jobs.c", freshness=["job.is_fresh:jobs.a"], refresh_propagation="block"),
        "jobs.d": _job("jobs.d", freshness=["job.is_fresh:jobs.b", "job.is_fresh:jobs.c"]),
    }


@pytest.mark.parametrize(
    "graph_factory,seed_root,expected_cleared,expected_warn_substr",
    [
        # block on root: warning, no clears (block wins)
        (_block_root_graph, "jobs.a", set(), "block"),
        # block in chain: a and b cleared, c (block) and d (unreachable) untouched
        (_block_in_chain_graph, "jobs.a", {"jobs.a", "jobs.b"}, None),
        # block in diamond: d still reached via the auto branch
        (_block_in_diamond_graph, "jobs.a", {"jobs.a", "jobs.b", "jobs.d"}, None),
    ],
    ids=["block-on-root", "block-in-chain", "block-in-diamond"],
)
def test_eager_cascade_block_semantics(
    runner_state: Dict[str, TJobDefinition],
    graph_factory: Callable[[], Dict[str, TJobDefinition]],
    seed_root: str,
    expected_cleared: Set[str],
    expected_warn_substr: Optional[str],
) -> None:
    """`_eager_refresh_cascade` honors `block` policy on roots and mid-walk."""
    graph = graph_factory()
    runner_state.update(graph)
    seed_ts = _dt("2024-06-01T00:00:00Z")
    for ref in graph:
        runner_mod._freshness_store.set_prev_completed_run(ref, seed_ts)

    warnings: List[str] = []
    runner_mod._eager_refresh_cascade(
        [(graph[seed_root], TTrigger(f"manual:{seed_root}"))],
        warn=warnings.append,
    )

    if expected_warn_substr is None:
        assert warnings == []
    else:
        assert len(warnings) == 1
        assert expected_warn_substr in warnings[0]
    for ref in graph:
        actual = runner_mod._freshness_store.get_prev_completed_run(ref)
        if ref in expected_cleared:
            assert actual is None, f"{ref} should have been cleared"
        else:
            assert actual == seed_ts, f"{ref} should have been left alone"


def _always_chain_graph() -> Dict[str, TJobDefinition]:
    return {
        "jobs.a": _job("jobs.a", refresh_propagation="always"),
        "jobs.b": _job("jobs.b", freshness=["job.is_fresh:jobs.a"]),
        "jobs.c": _job("jobs.c", freshness=["job.is_fresh:jobs.b"]),
    }


def _always_then_block_graph() -> Dict[str, TJobDefinition]:
    return {
        "jobs.a": _job("jobs.a", refresh_propagation="always"),
        "jobs.b": _job("jobs.b", freshness=["job.is_fresh:jobs.a"], refresh_propagation="block"),
    }


def _auto_chain_graph() -> Dict[str, TJobDefinition]:
    return {
        "jobs.a": _job("jobs.a"),
        "jobs.b": _job("jobs.b", freshness=["job.is_fresh:jobs.a"]),
    }


@pytest.mark.parametrize(
    "graph_factory,start_ref,expected_cleared",
    [
        # always upstream clears its transitive downstream at start
        (_always_chain_graph, "jobs.a", {"jobs.b", "jobs.c"}),
        # always upstream + block downstream: block wins, b untouched
        (_always_then_block_graph, "jobs.a", set()),
        # auto upstream is transparent — no cascade fires at start
        (_auto_chain_graph, "jobs.a", set()),
    ],
    ids=["always-chain", "always-then-block", "auto-no-cascade"],
)
def test_start_job_policy_cascade(
    runner_state: Dict[str, TJobDefinition],
    stubbed_job_process: List[List[str]],
    graph_factory: Callable[[], Dict[str, TJobDefinition]],
    start_ref: str,
    expected_cleared: Set[str],
) -> None:
    """`_start_job` fires the always-cascade and never touches the root itself."""
    graph = graph_factory()
    runner_state.update(graph)
    seed_ts = _dt("2024-06-01T00:00:00Z")
    for ref in graph:
        runner_mod._freshness_store.set_prev_completed_run(ref, seed_ts)

    runner_mod._start_job(
        graph[start_ref],
        TTrigger(f"manual:{start_ref}"),
        port_counter=[8000],
    )

    for ref in graph:
        actual = runner_mod._freshness_store.get_prev_completed_run(ref)
        if ref in expected_cleared:
            assert actual is None, f"{ref} should have been cleared by cascade"
        else:
            assert actual == seed_ts, f"{ref} should have been left alone"


def test_pokemon_chain_settles_after_one_cascade(
    runner_state: Dict[str, TJobDefinition],
    stubbed_job_process: List[List[str]],
) -> None:
    """Regression: `always → auto → auto` chain must not re-fire on each completion.

    Reproduces the `pokemon_pipeline.py` bug where the OLD completion-time
    `auto + was_refresh` propagation kept clearing the downstream
    `prev_completed_run` on every completion of the `always` root, leaving
    the leaf permanently in `refresh=True` state. Under the new
    eager-only design, `_on_non_interval_success` only advances the
    completing job's watermark and never touches downstream — so the
    chain settles after each cascade fires.
    """
    runner_state["jobs.backfill"] = _job("jobs.backfill", refresh_propagation="always")
    runner_state["jobs.daily"] = _job("jobs.daily", freshness=["job.is_fresh:jobs.backfill"])
    runner_state["jobs.transform"] = _job("jobs.transform", freshness=["job.is_fresh:jobs.daily"])

    # cycle 1: backfill starts → cascade clears daily and transform
    runner_mod._start_job(
        runner_state["jobs.backfill"],
        TTrigger("manual:jobs.backfill"),
        port_counter=[8000],
    )
    assert runner_mod._freshness_store.get_prev_completed_run("jobs.daily") is None
    assert runner_mod._freshness_store.get_prev_completed_run("jobs.transform") is None

    # backfill completes → only its own watermark advances
    runner_mod._on_non_interval_success(
        "jobs.backfill", _run_record("jobs.backfill", _dt("2024-06-15T12:00:00Z"))
    )
    assert runner_mod._freshness_store.get_prev_completed_run("jobs.backfill") == _dt(
        "2024-06-15T12:00:00Z"
    )
    assert runner_mod._freshness_store.get_prev_completed_run("jobs.daily") is None
    assert runner_mod._freshness_store.get_prev_completed_run("jobs.transform") is None

    # daily runs and completes — its policy is auto, no cascade fires
    runner_mod._start_job(
        runner_state["jobs.daily"],
        TTrigger("manual:jobs.daily"),
        port_counter=[8000],
    )
    runner_mod._on_non_interval_success(
        "jobs.daily", _run_record("jobs.daily", _dt("2024-06-15T12:05:00Z"))
    )
    assert runner_mod._freshness_store.get_prev_completed_run("jobs.daily") == _dt(
        "2024-06-15T12:05:00Z"
    )
    # transform was cleared by the cascade in step 1; daily's auto policy
    # does NOT re-clear it on completion
    assert runner_mod._freshness_store.get_prev_completed_run("jobs.transform") is None

    # transform runs and completes
    runner_mod._start_job(
        runner_state["jobs.transform"],
        TTrigger("manual:jobs.transform"),
        port_counter=[8000],
    )
    runner_mod._on_non_interval_success(
        "jobs.transform",
        _run_record("jobs.transform", _dt("2024-06-15T12:10:00Z")),
    )
    assert runner_mod._freshness_store.get_prev_completed_run("jobs.transform") == _dt(
        "2024-06-15T12:10:00Z"
    )

    # cycle 2: backfill restarts → cascade re-clears daily and transform
    # (this IS expected — every start of an always job re-cascades)
    runner_mod._start_job(
        runner_state["jobs.backfill"],
        TTrigger("manual:jobs.backfill"),
        port_counter=[8000],
    )
    assert runner_mod._freshness_store.get_prev_completed_run("jobs.daily") is None
    assert runner_mod._freshness_store.get_prev_completed_run("jobs.transform") is None

    # the regression: under the OLD design, daily completing as a refresh run
    # would re-clear transform via the auto+was_refresh propagation, looping
    # forever. Under the new design, daily's completion only advances daily.
    # Pre-set transform to a known watermark and verify daily's completion
    # leaves it untouched.
    runner_mod._freshness_store.set_prev_completed_run(
        "jobs.transform", _dt("2024-06-15T13:08:00Z")
    )
    runner_mod._on_non_interval_success(
        "jobs.daily", _run_record("jobs.daily", _dt("2024-06-15T13:05:00Z"))
    )
    assert runner_mod._freshness_store.get_prev_completed_run("jobs.transform") == _dt(
        "2024-06-15T13:08:00Z"
    )
