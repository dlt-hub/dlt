"""Unit tests for the unified run/serve banner, warnings, and picker."""

from typing import List, Tuple

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


def test_print_run_banner_local_includes_chip_and_fields(
    capsys: pytest.CaptureFixture[str],
) -> None:
    info: TRunBannerInfo = {
        "display_label": "etl_daily",
        "job_ref": "jobs.etl.daily",
        "trigger": "schedule:0 0 * * *",
        "trigger_humanized": "schedule: 0 0 * * *",
        "profile": "dev",
        "location": "local",
        "workspace_name": "my_ws",
    }
    print_run_banner(info)
    out = capsys.readouterr().out
    assert "Starting" in out and "etl_daily" in out
    assert "local" in out
    assert "jobs.etl.daily" in out
    assert "schedule: 0 0 * * *" in out
    assert "dev" in out
    assert "my_ws" in out
    assert "Listening on" not in out


def test_print_run_banner_remote_with_port(capsys: pytest.CaptureFixture[str]) -> None:
    info: TRunBannerInfo = {
        "display_label": "notebook",
        "job_ref": "jobs.notebook",
        "trigger": "manual:jobs.notebook",
        "trigger_humanized": "manual",
        "profile": "access",
        "location": "remote",
        "port": 5000,
    }
    print_run_banner(info)
    out = capsys.readouterr().out
    assert "remote" in out
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


def test_pick_one_job_single_match_returns_it_without_prompt() -> None:
    cands = [_candidate("jobs.a")]
    jd, _ = pick_one_job(cands)
    assert jd["job_ref"] == "jobs.a"


def test_pick_one_job_non_tty_raises_ambiguous(monkeypatch: pytest.MonkeyPatch) -> None:
    """Non-tty must NEVER silently pick — raises so the agent re-runs with --job-ref."""
    cands = [_candidate("jobs.a"), _candidate("jobs.b")]
    # echo's interactivity is already non-interactive in non-tty harness; explicit:
    monkeypatch.setattr(fmt, "ALWAYS_CHOOSE_DEFAULT", True)
    with pytest.raises(AmbiguousJobSelector):
        pick_one_job(cands)


def test_pick_one_job_non_interactive_flag_raises_even_in_tty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`--non-interactive` flips `is_interactive()` False even on tty — must raise, never silently pick."""
    cands = [_candidate("jobs.a"), _candidate("jobs.b")]

    class _FakeStream:
        def isatty(self) -> bool:
            return True

    monkeypatch.setattr("sys.stdin", _FakeStream())
    monkeypatch.setattr("sys.stdout", _FakeStream())
    monkeypatch.setattr(fmt, "ALWAYS_CHOOSE_DEFAULT", True)
    with pytest.raises(AmbiguousJobSelector):
        pick_one_job(cands)
