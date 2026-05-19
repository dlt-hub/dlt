"""Unit tests for the unified run/serve banner, warnings, plan, and picker."""

from typing import Tuple

import pytest

from dlt._workspace.cli import echo as fmt
from dlt._workspace.deployment._run_typing import TRunBannerInfo, TRunJobInfo
from dlt._workspace.deployment._run_views import (
    pick_one_job,
    print_run_banner,
    print_run_plan,
    print_run_warnings,
)
from dlt._workspace.deployment.exceptions import AmbiguousJobSelector
from dlt._workspace.deployment.typing import (
    TEntryPoint,
    TExecuteSpec,
    TJobDefinition,
    TJobRef,
    TTrigger,
)


def _candidate(ref: str) -> Tuple[TJobDefinition, TTrigger]:
    entry: TEntryPoint = {
        "module": "m",
        "function": "main",
        "job_type": "batch",
        "launcher": "dlt._workspace.deployment.launchers.job",
    }
    jd: TJobDefinition = {
        "job_ref": TJobRef(ref),
        "entry_point": entry,
        "triggers": [TTrigger(f"manual:{ref}")],
        "execute": TExecuteSpec(),
    }
    return jd, TTrigger(f"manual:{ref}")


def test_print_run_banner_renders_every_line(capsys: pytest.CaptureFixture[str]) -> None:
    """Single banner test — all fields populated, every line must be present."""
    info: TRunBannerInfo = {
        "display_label": "etl_daily",
        "job_ref": "jobs.etl.daily",
        "trigger": "schedule:0 0 * * *",
        "trigger_humanized": "schedule: 0 0 * * *",
        "profile": "dev",
        "location": "local",
        "workspace_name": "my_ws",
        "run_id": "abc123",
        "port": 5000,
    }
    print_run_banner(info)
    out = capsys.readouterr().out
    # "Starting <label>  [<chip>]" header
    assert "Starting" in out and "etl_daily" in out
    assert "local" in out
    # one line per field
    assert "job_ref:" in out and "jobs.etl.daily" in out
    assert "trigger:" in out and "schedule: 0 0 * * *" in out
    assert "profile:" in out and "dev" in out
    assert "workspace:" in out and "my_ws" in out
    assert "run_id:" in out and "abc123" in out
    # interactive port line
    assert "Listening on http://localhost:5000" in out


def test_print_run_warnings_emits_each(capsys: pytest.CaptureFixture[str]) -> None:
    print_run_warnings(
        ["manifest warn 1", "manifest warn 2"],
        refresh_warning="refresh blocked",
        profile_warning="profile mismatch",
    )
    captured = capsys.readouterr()
    combined = captured.out + captured.err
    assert "manifest warn 1" in combined
    assert "manifest warn 2" in combined
    assert "refresh blocked" in combined
    assert "profile mismatch" in combined


def test_print_run_plan_renders_entry_point(capsys: pytest.CaptureFixture[str]) -> None:
    info: TRunJobInfo = {
        "job_ref": "jobs.a",
        "display_label": "jobs.a",
        "trigger": "manual:jobs.a",
        "trigger_humanized": "manual",
        "launcher": "dlt._workspace.deployment.launchers.job",
        "run_id": "abc-123",
        "entry_point": {"module": "m", "profile": "dev"},
        "manifest_warnings": [],
    }
    print_run_plan(info)
    out = capsys.readouterr().out
    assert "job_ref: jobs.a" in out
    assert "trigger: manual:jobs.a" in out
    assert "run_id:" in out and "abc-123" in out
    assert '"profile": "dev"' in out


@pytest.mark.parametrize(
    "scenario",
    ["single-match-passes-through", "non-tty-raises", "non-interactive-tty-raises"],
)
def test_pick_one_job(monkeypatch: pytest.MonkeyPatch, scenario: str) -> None:
    """Single candidate passes through; any non-interactive context raises (never silently picks)."""
    if scenario == "single-match-passes-through":
        jd, _ = pick_one_job([_candidate("jobs.a")])
        assert jd["job_ref"] == "jobs.a"
        return

    cands = [_candidate("jobs.a"), _candidate("jobs.b")]
    if scenario == "non-interactive-tty-raises":
        # `--non-interactive` flips fmt.is_interactive() False even when streams are tty
        class _FakeStream:
            def isatty(self) -> bool:
                return True

        monkeypatch.setattr("sys.stdin", _FakeStream())
        monkeypatch.setattr("sys.stdout", _FakeStream())
    monkeypatch.setattr(fmt, "ALWAYS_CHOOSE_DEFAULT", True)
    with pytest.raises(AmbiguousJobSelector):
        pick_one_job(cands)
