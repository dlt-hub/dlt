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


def test_select_jobs_direct_only() -> None:
    jobs = [_batch_job("jobs.mod.a"), _batch_job("jobs.mod.b")]
    selected, followup = select_jobs(jobs, ["manual:jobs.mod.a"])
    assert len(selected) == 1
    assert selected[0][0]["job_ref"] == "jobs.mod.a"
    # only the manual trigger matched
    assert selected[0][1] == [TTrigger("manual:jobs.mod.a")]
    assert followup == []


def test_select_jobs_no_match() -> None:
    selected, followup = select_jobs([_batch_job("jobs.mod.a")], ["tag:nonexistent"])
    assert selected == []
    assert followup == []


def test_select_jobs_multiple_triggers_matched() -> None:
    """Multiple selectors match multiple triggers on the same job."""
    job = _batch_job(
        "jobs.mod.x", triggers=["tag:daily", "manual:jobs.mod.x", "schedule:0 8 * * *"]
    )
    selected, _ = select_jobs([job], ["tag:*", "schedule:*"])
    assert len(selected) == 1
    matched = selected[0][1]
    assert TTrigger("tag:daily") in matched
    assert TTrigger("schedule:0 8 * * *") in matched
    # manual NOT matched by these selectors
    assert TTrigger("manual:jobs.mod.x") not in matched


def test_select_jobs_with_followup() -> None:
    """Followup jobs are collected transitively with their event triggers."""
    upstream = _batch_job("jobs.mod.upstream")
    middle = _batch_job(
        "jobs.mod.middle",
        triggers=["job.success:jobs.mod.upstream", "manual:jobs.mod.middle"],
    )
    leaf = _batch_job("jobs.mod.leaf", triggers=["job.success:jobs.mod.middle"])
    unrelated = _batch_job("jobs.mod.unrelated")

    selected, followup = select_jobs(
        [upstream, middle, leaf, unrelated],
        ["manual:jobs.mod.upstream"],
    )
    assert len(selected) == 1
    followup_refs = {j["job_ref"] for j, _ in followup}
    assert followup_refs == {"jobs.mod.middle", "jobs.mod.leaf"}

    # followup only has event triggers, not manual
    for _, triggers in followup:
        for t in triggers:
            assert t.startswith("job.success:") or t.startswith("job.fail:")


def test_select_jobs_followup_not_directly_selected() -> None:
    upstream = _batch_job("jobs.mod.up")
    downstream = _batch_job(
        "jobs.mod.down",
        triggers=["job.success:jobs.mod.up", "manual:jobs.mod.down"],
    )
    selected, followup = select_jobs([upstream, downstream], ["manual:jobs.mod.up"])
    assert len(selected) == 1
    assert selected[0][0]["job_ref"] == "jobs.mod.up"
    assert len(followup) == 1
    assert followup[0][0]["job_ref"] == "jobs.mod.down"
    # only event trigger in followup, not manual
    assert followup[0][1] == [TTrigger("job.success:jobs.mod.up")]


def test_select_jobs_unreachable_excluded() -> None:
    """Event-triggered job whose upstream is not selected is excluded."""
    job_a = _batch_job("jobs.mod.a")
    job_b = _batch_job("jobs.mod.b")
    downstream = _batch_job("jobs.mod.down", triggers=["job.success:jobs.mod.b"])
    selected, followup = select_jobs([job_a, job_b, downstream], ["manual:jobs.mod.a"])
    assert len(selected) == 1
    assert followup == []


def test_with_future_selects_only_timed_triggers() -> None:
    """schedule:* selector matches schedule trigger but not manual."""
    job = _batch_job("jobs.mod.cron", triggers=["schedule:0 8 * * *", "manual:jobs.mod.cron"])
    manual_only = _batch_job("jobs.mod.other")

    selected, _ = select_jobs([job, manual_only], ["schedule:*"])
    assert len(selected) == 1
    assert selected[0][0]["job_ref"] == "jobs.mod.cron"
    # only schedule trigger matched, not manual
    assert selected[0][1] == [TTrigger("schedule:0 8 * * *")]


def test_with_future_and_followup() -> None:
    """Followup of timed jobs is collected."""
    every_job = _batch_job("jobs.mod.poll", triggers=["every:30s", "manual:jobs.mod.poll"])
    downstream = _batch_job(
        "jobs.mod.process",
        triggers=["job.success:jobs.mod.poll", "manual:jobs.mod.process"],
    )
    selected, followup = select_jobs([every_job, downstream], ["every:*"])
    assert len(selected) == 1
    assert selected[0][0]["job_ref"] == "jobs.mod.poll"
    assert selected[0][1] == [TTrigger("every:30s")]
    assert len(followup) == 1
    assert followup[0][0]["job_ref"] == "jobs.mod.process"


def test_with_future_followup_of_direct_and_timed() -> None:
    """Followup collects from both directly selected and timed jobs."""
    manual_job = _batch_job("jobs.mod.manual")
    cron_job = _batch_job("jobs.mod.cron", triggers=["schedule:0 8 * * *", "manual:jobs.mod.cron"])
    after_manual = _batch_job("jobs.mod.after_manual", triggers=["job.success:jobs.mod.manual"])
    after_cron = _batch_job("jobs.mod.after_cron", triggers=["job.success:jobs.mod.cron"])

    selected, followup = select_jobs(
        [manual_job, cron_job, after_manual, after_cron],
        ["manual:jobs.mod.manual", "schedule:*"],
    )
    selected_refs = {j["job_ref"] for j, _ in selected}
    assert selected_refs == {"jobs.mod.manual", "jobs.mod.cron"}
    followup_refs = {j["job_ref"] for j, _ in followup}
    assert followup_refs == {"jobs.mod.after_manual", "jobs.mod.after_cron"}


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


def test_with_future_does_not_fire_manual(capsys: pytest.CaptureFixture[str]) -> None:
    """--with-future selects timed jobs but does not fire their manual trigger."""
    scheduled = _batch_job(
        "jobs.mod.cron_job",
        triggers=["schedule:0 8 * * *", "manual:jobs.mod.cron_job"],
    )
    manual_only = _batch_job("jobs.mod.manual_only")

    exit_code = run([scheduled, manual_only], with_future=True, dry_run=True)
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "cron_job" in captured.err
    assert "scheduled" in captured.err
    assert "manual_only" not in captured.err or "run now" not in captured.err


def test_with_future_alone_no_selectors(capsys: pytest.CaptureFixture[str]) -> None:
    """--with-future without --select or --run-manual only runs timed jobs."""
    scheduled = _batch_job(
        "jobs.mod.cron_job",
        triggers=["schedule:0 8 * * *", "manual:jobs.mod.cron_job"],
    )
    manual_only = _batch_job("jobs.mod.manual_only")

    exit_code = run([scheduled, manual_only], with_future=True, dry_run=True)
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "selected 1 job(s)" in captured.err


@pytest.mark.parametrize(
    "selectors,expected",
    [
        (
            ["tag:backfill", "every:*", "schedule:*", "manual:*"],
            ["tag:backfill", "every:*", "schedule:*", "manual:*"],
        ),
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
