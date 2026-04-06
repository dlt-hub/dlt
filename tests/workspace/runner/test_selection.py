"""Tests for job selection, followup resolution, and selector handling."""

from typing import List, Optional

import pytest

from dlt._workspace._runner.runner import run, select_jobs, _resolve_selectors
from dlt._workspace.deployment.typing import (
    TEntryPoint,
    TExecuteSpec,
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
        "execute": TExecuteSpec(),
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
    every_job = _batch_job("jobs.mod.poll", triggers=["every:1m", "manual:jobs.mod.poll"])
    downstream = _batch_job(
        "jobs.mod.process",
        triggers=["job.success:jobs.mod.poll", "manual:jobs.mod.process"],
    )
    selected, followup = select_jobs([every_job, downstream], ["every:*"])
    assert len(selected) == 1
    assert selected[0][0]["job_ref"] == "jobs.mod.poll"
    assert selected[0][1] == [TTrigger("every:1m")]
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


@pytest.mark.parametrize(
    "trigger",
    ["every:1h", "schedule:0 8 * * *"],
    ids=["every", "schedule"],
)
def test_timed_triggers_auto_scheduled(capsys: pytest.CaptureFixture[str], trigger: str) -> None:
    """Timed triggers are scheduled automatically, none fire immediately."""
    job = _batch_job(
        "jobs.batch_jobs.poll",
        triggers=[trigger, "manual:jobs.batch_jobs.poll"],
        function="backfill",
    )
    exit_code = run([job], dry_run=True)
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "run now" not in captured.err


def test_manual_only_job_is_idle(capsys: pytest.CaptureFixture[str]) -> None:
    """Manual-only jobs are idle without --select."""
    scheduled = _batch_job(
        "jobs.mod.cron_job",
        triggers=["schedule:0 8 * * *", "manual:jobs.mod.cron_job"],
    )
    manual_only = _batch_job("jobs.mod.manual_only")

    exit_code = run([scheduled, manual_only], dry_run=True)
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "cron_job" in captured.err
    assert "idle" in captured.err


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
    job_refs = [TJobRef("jobs.mod.a")]
    assert _resolve_selectors(selectors, job_refs) == expected


@pytest.mark.parametrize(
    "selector,expected",
    [
        ("backfill", "manual:jobs.mod.backfill"),
        ("mod.backfill", "manual:jobs.mod.backfill"),
    ],
    ids=["bare-name", "section-name"],
)
def test_resolve_selectors_converts_job_ref(selector: str, expected: str) -> None:
    job_refs = [TJobRef("jobs.mod.backfill")]
    assert _resolve_selectors([selector], job_refs) == [expected]


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


@pytest.mark.parametrize(
    "trigger",
    ["tag:backfill", "deployment:", "manual:jobs.batch_jobs.backfill"],
    ids=["tag", "deployment", "manual"],
)
def test_selection_only_trigger_does_not_auto_fire(
    capsys: pytest.CaptureFixture[str], trigger: str
) -> None:
    """Tag, deployment, and manual triggers only fire via --select."""
    job = _batch_job(
        "jobs.batch_jobs.backfill",
        triggers=[trigger, "manual:jobs.batch_jobs.backfill"],
    )
    exit_code = run([job], no_future=True, dry_run=True)
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "run now" not in captured.err
    assert "nothing to run" in captured.err


@pytest.mark.parametrize(
    "trigger,selector",
    [
        ("tag:backfill", "tag:*"),
        ("manual:jobs.batch_jobs.backfill", "manual:*"),
    ],
    ids=["tag", "manual"],
)
def test_selection_only_trigger_fires_via_select(
    capsys: pytest.CaptureFixture[str], trigger: str, selector: str
) -> None:
    """Tag and manual triggers fire when matched by --select."""
    job = _batch_job(
        "jobs.batch_jobs.backfill",
        triggers=[trigger, "manual:jobs.batch_jobs.backfill"],
    )
    exit_code = run([job], trigger_selectors=[selector], no_future=True, dry_run=True)
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "run now" in captured.err


def test_pipeline_name_selector(capsys: pytest.CaptureFixture[str]) -> None:
    """Jobs with pipeline_name in deliver are matched by pipeline_name: selector."""
    job = _batch_job("jobs.batch_jobs.backfill")
    job["deliver"] = {"pipeline_name": "analytics"}
    exit_code = run(
        [job],
        trigger_selectors=["pipeline_name:analytics"],
        no_future=True,
        dry_run=True,
    )
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "run now" in captured.err


def test_pipeline_name_wildcard_selector(capsys: pytest.CaptureFixture[str]) -> None:
    """pipeline_name:* matches all jobs with a pipeline_name set."""
    job_a = _batch_job("jobs.batch_jobs.backfill")
    job_a["deliver"] = {"pipeline_name": "analytics"}
    job_b = _batch_job("jobs.batch_jobs.transform", function="transform")
    job_b["deliver"] = {"pipeline_name": "reporting"}
    job_c = _batch_job("jobs.batch_jobs.cleanup", function="backfill")
    # job_c has no pipeline_name
    exit_code = run(
        [job_a, job_b, job_c],
        trigger_selectors=["pipeline_name:*"],
        no_future=True,
        dry_run=True,
    )
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "backfill" in captured.err and "run now" in captured.err
    assert "transform" in captured.err


def test_unreachable_event_job_shows_as_waiting(capsys: pytest.CaptureFixture[str]) -> None:
    """Event-triggered job whose upstream won't fire still shows as 'waiting'."""
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
        no_future=True,
    )
    assert exit_code == 0
    captured = capsys.readouterr()
    # all jobs registered — downstream shows as waiting
    assert "downstream" in captured.err
    assert "waiting" in captured.err
