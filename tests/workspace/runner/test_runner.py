"""Tests for the local workspace runner."""

from typing import List, Optional

import pytest

from dlt._workspace._runner.runner import run, _log, _log_job
from dlt._workspace.deployment.typing import (
    TFreshnessConstraint,
    TJobDefinition,
    TJobRef,
    TEntryPoint,
    TExecuteSpec,
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
    """Non-matching selectors still show all jobs; manual-only are idle."""
    jobs = [_batch_job("jobs.batch_jobs.backfill")]
    exit_code = run(jobs, trigger_selectors=["tag:nonexistent"])
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "idle" in captured.err
    assert "nothing to run" in captured.err


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
            "execute": TExecuteSpec(),
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
        "execute": TExecuteSpec(),
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
    assert "dry run" in captured.err


def test_manifest_always_shown(capsys: pytest.CaptureFixture[str]) -> None:
    """Manifest summary is always displayed."""
    jobs = [_batch_job("jobs.batch_jobs.backfill")]
    run(jobs)
    captured = capsys.readouterr()
    assert "manifest: 1 job(s)" in captured.err
    assert "backfill" in captured.err


def test_freshness_blocks_when_upstream_not_run(capsys: pytest.CaptureFixture[str]) -> None:
    """Non-interval job with freshness constraint is skipped when upstream never ran."""
    upstream = _batch_job("jobs.batch_jobs.backfill")
    downstream = _batch_job(
        "jobs.batch_jobs.transform",
        triggers=["manual:jobs.batch_jobs.transform"],
        function="transform",
    )
    downstream["freshness"] = [TFreshnessConstraint("job.is_fresh:jobs.batch_jobs.backfill")]
    # only select downstream — upstream never runs, so freshness blocks
    exit_code = run([upstream, downstream], trigger_selectors=["manual:jobs.batch_jobs.transform"])
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "skipped" in captured.err
    assert "no completed runs" in captured.err


def test_freshness_passes_after_upstream_completes() -> None:
    """Non-interval job with freshness runs after upstream completes via event chain."""
    upstream = _batch_job("jobs.batch_jobs.backfill")
    downstream = _batch_job(
        "jobs.batch_jobs.transform",
        triggers=["job.success:jobs.batch_jobs.backfill"],
        function="transform",
    )
    downstream["freshness"] = [TFreshnessConstraint("job.is_fresh:jobs.batch_jobs.backfill")]
    # upstream runs, succeeds, fires event → downstream checks freshness → passes
    exit_code = run([upstream, downstream], trigger_selectors=["manual:*"])
    assert exit_code == 0


def test_freshness_blocks_interval_job_when_upstream_not_fresh(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Interval job with freshness constraint is skipped when upstream hasn't completed."""
    upstream: TJobDefinition = {
        "job_ref": TJobRef("jobs.batch_jobs.backfill"),
        "entry_point": TEntryPoint(
            module=f"{WORKSPACE}.batch_jobs",
            function="backfill",
            job_type="batch",
        ),
        "triggers": [TTrigger("schedule:* * * * *")],
        "execute": TExecuteSpec(),
        "interval": {"start": "2020-01-01T00:00:00Z"},
        "allow_external_schedulers": True,
        "default_trigger": TTrigger("schedule:* * * * *"),
    }
    downstream: TJobDefinition = {
        "job_ref": TJobRef("jobs.batch_jobs.transform"),
        "entry_point": TEntryPoint(
            module=f"{WORKSPACE}.batch_jobs",
            function="transform",
            job_type="batch",
        ),
        "triggers": [
            TTrigger("schedule:* * * * *"),
            TTrigger("manual:jobs.batch_jobs.transform"),
        ],
        "execute": TExecuteSpec(),
        "interval": {"start": "2020-01-01T00:00:00Z"},
        "allow_external_schedulers": True,
        "freshness": [TFreshnessConstraint("job.is_fresh:jobs.batch_jobs.backfill")],
        "default_trigger": TTrigger("schedule:* * * * *"),
    }
    # only select downstream — suppress scheduling so upstream never runs
    exit_code = run(
        [upstream, downstream],
        trigger_selectors=["manual:jobs.batch_jobs.transform"],
        no_future=True,
    )
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "skipped" in captured.err
    assert "not fully fresh" in captured.err


def test_log_formatting(capsys: pytest.CaptureFixture[str]) -> None:
    """_log goes to stderr, _log_job prefixes with job name."""
    _log("generic message")
    captured = capsys.readouterr()
    assert "generic message" in captured.err

    # stream_no=1: subprocess stdout goes to stdout
    _log_job("backfill", 1, "output line")
    captured = capsys.readouterr()
    assert "backfill" in captured.out
    assert "output line" in captured.out
    assert "|" in captured.out

    # stream_no=2: runner messages about jobs go to stderr
    _log_job("backfill", 2, "starting")
    captured = capsys.readouterr()
    assert "backfill" in captured.err
    assert "starting" in captured.err


def test_config_passed_to_job() -> None:
    """Config dict is passed through to job subprocess and resolves required params."""
    jobs = [
        _batch_job("jobs.batch_jobs.maintenance", function="maintenance"),
    ]
    # maintenance requires cleanup_days — pass via config
    exit_code = run(
        jobs,
        trigger_selectors=["manual:*"],
        config={"cleanup_days": "30"},
    )
    assert exit_code == 0


def test_config_missing_causes_failure() -> None:
    """Job fails when required config is not provided."""
    jobs = [
        _batch_job("jobs.batch_jobs.maintenance", function="maintenance"),
    ]
    # maintenance requires cleanup_days — omit config, job should fail
    exit_code = run(jobs, trigger_selectors=["manual:*"])
    assert exit_code == 1


def test_freshness_retry_after_upstream_completes() -> None:
    """Non-interval job blocked by freshness retries when upstream completes.

    Upstream and downstream both selected via manual:*. Downstream has
    freshness constraint on upstream. Both are dispatched immediately —
    downstream is skipped (upstream hasn't completed yet). When upstream
    completes, freshness listener fires, downstream retries and succeeds.
    """
    upstream = _batch_job("jobs.batch_jobs.backfill")
    upstream["default_trigger"] = TTrigger("manual:jobs.batch_jobs.backfill")
    downstream = _batch_job(
        "jobs.batch_jobs.transform",
        triggers=["manual:jobs.batch_jobs.transform"],
        function="transform",
    )
    downstream["freshness"] = [TFreshnessConstraint("job.is_fresh:jobs.batch_jobs.backfill")]
    downstream["default_trigger"] = TTrigger("manual:jobs.batch_jobs.transform")
    # both selected — upstream runs, downstream initially blocked, retries on upstream success
    exit_code = run([upstream, downstream], trigger_selectors=["manual:*"])
    assert exit_code == 0
