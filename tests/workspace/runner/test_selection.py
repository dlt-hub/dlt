"""Tests for job selection, followup resolution, and selector handling."""

from typing import List, Optional

import pytest

from dlt._workspace._runner.runner import run, select_jobs, _resolve_selectors
from dlt._workspace.deployment.typing import (
    TEntryPoint,
    TExecutionSpec,
    TJobDefinition,
    TJobRef,
    TTrigger,
)

WORKSPACE = "tests.workspace.cases.runtime_workspace"


def _batch_job(
    ref: str, triggers: Optional[List[str]] = None, function: Optional[str] = None
) -> TJobDefinition:
    if function is None:
        function = ref.rsplit(".", 1)[-1]
    if triggers is None:
        triggers = [f"manual:{ref}"]
    return {
        "job_ref": TJobRef(ref),
        "entry_point": TEntryPoint(
            module=f"{WORKSPACE}.batch_jobs",
            function=function,
            job_type="batch",
        ),
        "triggers": [TTrigger(t) for t in triggers],
        "execution": TExecutionSpec(),
        "starred": False,
    }


# ---- select_jobs ----


def test_select_jobs_direct_only() -> None:
    jobs = [_batch_job("jobs.mod.a"), _batch_job("jobs.mod.b")]
    direct, followup = select_jobs(jobs, ["manual:jobs.mod.a"])
    assert len(direct) == 1
    assert direct[0]["job_ref"] == "jobs.mod.a"
    assert followup == []


def test_select_jobs_with_followup() -> None:
    """Followup jobs are collected transitively."""
    upstream = _batch_job("jobs.mod.upstream")
    middle = _batch_job(
        "jobs.mod.middle",
        triggers=["job.success:jobs.mod.upstream", "manual:jobs.mod.middle"],
    )
    leaf = _batch_job("jobs.mod.leaf", triggers=["job.success:jobs.mod.middle"])
    unrelated = _batch_job("jobs.mod.unrelated")

    direct, followup = select_jobs(
        [upstream, middle, leaf, unrelated],
        ["manual:jobs.mod.upstream"],
    )
    assert len(direct) == 1
    assert direct[0]["job_ref"] == "jobs.mod.upstream"
    followup_refs = {j["job_ref"] for j in followup}
    assert followup_refs == {"jobs.mod.middle", "jobs.mod.leaf"}


def test_select_jobs_followup_not_directly_selected() -> None:
    upstream = _batch_job("jobs.mod.up")
    downstream = _batch_job(
        "jobs.mod.down",
        triggers=["job.success:jobs.mod.up", "manual:jobs.mod.down"],
    )
    direct, followup = select_jobs([upstream, downstream], ["manual:jobs.mod.up"])
    assert len(direct) == 1
    assert direct[0]["job_ref"] == "jobs.mod.up"
    assert len(followup) == 1
    assert followup[0]["job_ref"] == "jobs.mod.down"


def test_select_jobs_unreachable_excluded() -> None:
    """Event-triggered job whose upstream is not selected is excluded."""
    job_a = _batch_job("jobs.mod.a")
    job_b = _batch_job("jobs.mod.b")
    downstream = _batch_job("jobs.mod.down", triggers=["job.success:jobs.mod.b"])
    direct, followup = select_jobs([job_a, job_b, downstream], ["manual:jobs.mod.a"])
    assert len(direct) == 1
    assert followup == []


def test_select_jobs_no_match() -> None:
    direct, followup = select_jobs([_batch_job("jobs.mod.a")], ["tag:nonexistent"])
    assert direct == []
    assert followup == []


# ---- with-future selects timed jobs ----


@pytest.mark.parametrize(
    "trigger,selector",
    [
        ("every:30s", "every:*"),
        ("schedule:0 8 * * *", "schedule:*"),
        ("once:2026-06-01T00:00:00Z", "once:*"),
    ],
    ids=["every", "schedule", "once"],
)
def test_with_future_selects_timed_jobs(trigger: str, selector: str) -> None:
    timed_job = _batch_job("jobs.mod.timed", triggers=[trigger, "manual:jobs.mod.timed"])
    other = _batch_job("jobs.mod.other")

    direct, _ = select_jobs([timed_job, other], [selector])
    assert len(direct) == 1
    assert direct[0]["job_ref"] == "jobs.mod.timed"


def test_with_future_and_followup() -> None:
    """--with-future selects timed job, its downstream follows via event trigger."""
    every_job = _batch_job("jobs.mod.poll", triggers=["every:30s", "manual:jobs.mod.poll"])
    downstream = _batch_job(
        "jobs.mod.process",
        triggers=["job.success:jobs.mod.poll", "manual:jobs.mod.process"],
    )
    direct, followup = select_jobs([every_job, downstream], ["every:*"])
    assert len(direct) == 1
    assert direct[0]["job_ref"] == "jobs.mod.poll"
    assert len(followup) == 1
    assert followup[0]["job_ref"] == "jobs.mod.process"


def test_with_future_flag_integration(capsys: pytest.CaptureFixture[str]) -> None:
    """--with-future flag causes every: job to be selected and shown in plan."""
    every_job = _batch_job(
        "jobs.batch_jobs.poll",
        triggers=["every:1h", "manual:jobs.batch_jobs.poll"],
        function="backfill",
    )
    exit_code = run([every_job], with_future=True, dry_run=True)
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "selected" in captured.err
    assert "run now" in captured.err


# ---- _resolve_selectors ----


@pytest.mark.parametrize(
    "selectors,expected",
    [
        # trigger patterns pass through
        (
            ["tag:backfill", "every:*", "schedule:*", "manual:*"],
            ["tag:backfill", "every:*", "schedule:*", "manual:*"],
        ),
        # job type keywords pass through
        (["batch", "interactive"], ["batch", "interactive"]),
    ],
    ids=["trigger-patterns", "type-keywords"],
)
def test_resolve_selectors_passthrough(selectors, expected) -> None:
    jobs = [_batch_job("jobs.mod.a")]
    assert _resolve_selectors(selectors, jobs) == expected


def test_resolve_selectors_converts_bare_name() -> None:
    jobs = [_batch_job("jobs.mod.backfill")]
    assert _resolve_selectors(["backfill"], jobs) == ["manual:jobs.mod.backfill"]


def test_resolve_selectors_converts_section_name() -> None:
    jobs = [_batch_job("jobs.mod.backfill")]
    assert _resolve_selectors(["mod.backfill"], jobs) == ["manual:jobs.mod.backfill"]


# ---- integration: followup runs via event, not manual ----


def test_followup_runs_via_event_not_manual(capsys: pytest.CaptureFixture[str]) -> None:
    """Followup job waits for upstream, does not fire immediately via manual."""
    upstream = _batch_job("jobs.batch_jobs.backfill")
    downstream = _batch_job(
        "jobs.batch_jobs.transform",
        triggers=["job.success:jobs.batch_jobs.backfill", "manual:jobs.batch_jobs.transform"],
        function="transform",
    )
    exit_code = run(
        [upstream, downstream],
        trigger_selectors=["manual:jobs.batch_jobs.backfill"],
    )
    assert exit_code == 0


def test_followup_excluded_when_unreachable(capsys: pytest.CaptureFixture[str]) -> None:
    """Event-triggered job not reachable from selected jobs is excluded."""
    job_a = _batch_job("jobs.mod.backfill")
    job_b = _batch_job("jobs.mod.daily", triggers=["tag:daily"])
    job_b["entry_point"]["function"] = "backfill"
    downstream = _batch_job(
        "jobs.mod.downstream",
        triggers=["job.success:jobs.mod.daily"],
        function="transform",
    )
    exit_code = run(
        [job_a, job_b, downstream],
        trigger_selectors=["manual:jobs.mod.backfill"],
    )
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "downstream" not in captured.err or "waiting" not in captured.err
