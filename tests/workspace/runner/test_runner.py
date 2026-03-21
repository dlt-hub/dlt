"""Tests for the local workspace runner."""

from typing import List, Optional

import pytest

from dlt._workspace._runner.runner import run, _log, _log_job
from dlt._workspace.deployment.typing import (
    TJobDefinition,
    TJobRef,
    TEntryPoint,
    TExecutionSpec,
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


def test_run_nothing_by_default(capsys: pytest.CaptureFixture[str]) -> None:
    """No selectors and no --run-manual shows manifest only."""
    jobs = [_batch_job("jobs.batch_jobs.backfill")]
    exit_code = run(jobs)
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "manifest:" in captured.err
    assert "nothing to run" in captured.err


def test_run_manual() -> None:
    """--select manual:* triggers all jobs with manual trigger."""
    jobs = [_batch_job("jobs.batch_jobs.backfill")]
    exit_code = run(jobs, trigger_selectors=["manual:*"])
    assert exit_code == 0


def test_run_single_batch_job() -> None:
    """Run a single batch job via --select with job ref."""
    jobs = [_batch_job("jobs.batch_jobs.backfill")]
    exit_code = run(jobs, trigger_selectors=["manual:*"])
    assert exit_code == 0


def test_run_chained_jobs() -> None:
    """Run jobs with job.success dependency chain — followup auto-included."""
    jobs = [
        _batch_job("jobs.batch_jobs.backfill"),
        _batch_job(
            "jobs.batch_jobs.transform",
            triggers=["job.success:jobs.batch_jobs.backfill"],
            function="transform",
        ),
    ]
    exit_code = run(jobs, trigger_selectors=["manual:*"])
    assert exit_code == 0


def test_run_with_no_matching_selectors(capsys: pytest.CaptureFixture[str]) -> None:
    """No matching selectors returns 0 with no jobs run."""
    jobs = [_batch_job("jobs.batch_jobs.backfill")]
    exit_code = run(jobs, trigger_selectors=["tag:nonexistent"])
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "no jobs matched" in captured.err


def test_run_with_tag_selector() -> None:
    """Tag selector matches jobs with matching triggers."""
    jobs = [
        _batch_job("jobs.batch_jobs.tagged", triggers=["tag:test"]),
    ]
    # tagged job needs a function that exists
    jobs[0]["entry_point"]["function"] = "backfill"
    exit_code = run(jobs, trigger_selectors=["tag:*"])
    assert exit_code == 0


def test_run_failing_job() -> None:
    """Failing job returns exit code 1."""
    jobs: List[TJobDefinition] = [
        {
            "job_ref": TJobRef("jobs.test.failing"),
            "entry_point": TEntryPoint(
                module="tests.workspace.runner.cases.failing_job",
                function="fail",
                job_type="batch",
            ),
            "triggers": [TTrigger("manual:jobs.test.failing")],
            "execution": TExecutionSpec(),
            "starred": False,
        }
    ]
    exit_code = run(jobs, trigger_selectors=["manual:*"])
    assert exit_code == 1


def test_orphaned_event_trigger_exits() -> None:
    """Job waiting on failed upstream's success exits with warning."""
    failing: TJobDefinition = {
        "job_ref": TJobRef("jobs.test.upstream"),
        "entry_point": TEntryPoint(
            module="tests.workspace.runner.cases.failing_job",
            function="fail",
            job_type="batch",
        ),
        "triggers": [TTrigger("manual:jobs.test.upstream")],
        "execution": TExecutionSpec(),
        "starred": False,
    }
    downstream = _batch_job(
        "jobs.test.downstream",
        triggers=["job.success:jobs.test.upstream"],
        function="backfill",
    )
    # upstream runs and fails, downstream never fires
    exit_code = run([failing, downstream], trigger_selectors=["manual:*"])
    assert exit_code == 1


def test_chained_job_runs_via_tag_selector() -> None:
    """When selecting by tag, downstream event-triggered jobs still fire."""
    tag_job = _batch_job("jobs.mod.daily", triggers=["tag:daily"])
    tag_job["entry_point"]["function"] = "backfill"
    chained = _batch_job(
        "jobs.mod.transform",
        triggers=["job.success:jobs.mod.daily"],
        function="transform",
    )
    exit_code = run([tag_job, chained], trigger_selectors=["tag:*"])
    assert exit_code == 0


def test_chained_with_skipped_upstream() -> None:
    """Downstream fires when one upstream succeeds, even if another upstream is skipped."""
    manual_job = _batch_job("jobs.mod.backfill")
    tag_job = _batch_job("jobs.mod.daily", triggers=["tag:daily"])
    tag_job["entry_point"]["function"] = "backfill"
    chained = _batch_job(
        "jobs.mod.transform",
        triggers=[
            "job.success:jobs.mod.backfill",
            "job.success:jobs.mod.daily",
        ],
        function="transform",
    )
    # select only the manual backfill — daily is not selected,
    # but backfill runs, succeeds, and triggers transform via event
    exit_code = run(
        [manual_job, tag_job, chained],
        trigger_selectors=["manual:jobs.mod.backfill"],
    )
    assert exit_code == 0


def test_select_by_job_ref(capsys: pytest.CaptureFixture[str]) -> None:
    """--select with a bare job name resolves to manual: selector."""
    jobs = [_batch_job("jobs.batch_jobs.backfill")]
    exit_code = run(jobs, trigger_selectors=["backfill"])
    assert exit_code == 0


def test_dry_run(capsys: pytest.CaptureFixture[str]) -> None:
    """Dry run displays plan without launching jobs."""
    jobs = [
        _batch_job("jobs.batch_jobs.backfill"),
        _batch_job(
            "jobs.batch_jobs.transform",
            triggers=["job.success:jobs.batch_jobs.backfill"],
            function="transform",
        ),
    ]
    exit_code = run(jobs, trigger_selectors=["manual:*"], dry_run=True)
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "run now" in captured.err
    assert "waiting" in captured.err
    assert "dry run" in captured.err


def test_manifest_always_shown(capsys: pytest.CaptureFixture[str]) -> None:
    """Manifest summary is always displayed."""
    jobs = [_batch_job("jobs.batch_jobs.backfill")]
    run(jobs)
    captured = capsys.readouterr()
    assert "manifest: 1 job(s)" in captured.err
    assert "backfill" in captured.err


def test_log_formatting(capsys: pytest.CaptureFixture[str]) -> None:
    """_log goes to stderr, _log_job prefixes with job name."""
    _log("generic message")
    captured = capsys.readouterr()
    assert "generic message" in captured.err

    # stream_no=1: subprocess stdout goes to stdout
    _log_job("backfill", 12, 1, "output line")
    captured = capsys.readouterr()
    assert "backfill" in captured.out
    assert "output line" in captured.out
    assert "|" in captured.out

    # stream_no=2: runner messages about jobs go to stderr
    _log_job("backfill", 12, 2, "starting")
    captured = capsys.readouterr()
    assert "backfill" in captured.err
    assert "starting" in captured.err
