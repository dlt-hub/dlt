"""Unit tests for the unified run/serve banner, warnings, plan, picker and agent transcript."""

import sys
from typing import Any, List, Optional, Tuple, cast

import click
import pytest

from dlt._workspace.cli import echo as fmt
from dlt._workspace.deployment._run_typing import TAgentEvent, TRunBannerInfo, TRunJobInfo
from dlt._workspace.deployment._run_views import (
    emit_agent_event,
    pick_one_job,
    print_agent_event,
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


def _agent_event(kind: str, **fields: Any) -> TAgentEvent:
    return cast(TAgentEvent, {"kind": kind, "agent": "job-inspector", **fields})


def test_print_agent_event_renders_the_transcript(capsys: pytest.CaptureFixture[str]) -> None:
    """One assertion per kind: what a person watching the run must see."""
    for event in [
        _agent_event("start", model="claude-sonnet-5", limits="max 30 turns"),
        _agent_event("prompt", text="Investigate run '89826ee6'."),
        _agent_event("mcp", text="dlt-workspace-mcp connected"),
        _agent_event("turn", turn=1, input_tokens=1204, output_tokens=340),
        _agent_event("thinks", text="the run id is there"),
        _agent_event("tool_call", tool="list_runs", server="dlt-workspace-mcp", detail={"n": 3}),
        _agent_event("tool_result", tool="list_runs", detail="3 runs"),
        _agent_event("says", text="The cursor produced duplicates."),
        _agent_event(
            "finish",
            status="succeeded",
            turn=3,
            total_tokens=7955,
            cost_usd=0.04,
            tools=["Read"],
            mcp_tools=["list_runs"],
        ),
    ]:
        print_agent_event(event)
    # colors are always on, so they are stripped to match the text
    out = click.unstyle(capsys.readouterr().out)

    assert "── job-inspector " in out and "claude-sonnet-5 · max 30 turns" in out
    assert "prompt\n  Investigate run '89826ee6'." in out
    assert "mcp  dlt-workspace-mcp connected" in out
    assert "turn 1" in out and "1,204 in / 340 out" in out
    assert "thinks  the run id is there" in out
    assert "list_runs (dlt-workspace-mcp)" in out and '{"n":3}' in out
    assert "→ 3 runs" in out
    assert "says\n  The cursor produced duplicates." in out
    assert "── succeeded " in out and "3 turns · 7,955 tokens · $0.04" in out
    assert "tools: Read · mcp tools: list_runs" in out


@pytest.mark.parametrize(
    "verbosity,thinking,detail",
    [(0, False, 80), (1, True, 200), (2, True, None)],
    ids=["quiet", "default", "everything"],
)
def test_agent_verbosity_caps_thinking_and_detail(
    capsys: pytest.CaptureFixture[str], verbosity: int, thinking: bool, detail: Optional[int]
) -> None:
    print_agent_event(_agent_event("thinks", text="t" * 500), verbosity)
    print_agent_event(_agent_event("tool_result", tool="read", detail="r" * 500), verbosity)
    out = capsys.readouterr().out

    assert ("t" * 20 in out) is thinking
    assert ("r" * 500 in out) is (detail is None)
    if detail is not None:
        assert f"{'r' * detail}…" in out


def test_a_failing_tool_result_is_red(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(sys.stdout, "isatty", lambda: True, raising=False)
    print_agent_event(_agent_event("tool_result", tool="run_bash", detail="boom", error=True))
    print_agent_event(_agent_event("tool_result", tool="run_bash", detail="fine"))
    red, green = capsys.readouterr().out.splitlines()

    assert fmt.style("→", fg="red") in red
    assert fmt.style("→", fg="green") in green


def test_agent_events_print_in_color_without_a_terminal(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A runner has no terminal, but its log viewer renders ANSI: the transcript keeps its colors."""
    monkeypatch.delenv("NO_COLOR", raising=False)
    monkeypatch.setattr(sys.stdout, "isatty", lambda: False, raising=False)
    emit_agent_event(_agent_event("finish", status="succeeded", turn=3, total_tokens=7955))
    out = capsys.readouterr().out
    assert "3 turns · 7,955 tokens" in out
    assert fmt.style("succeeded", fg="green") in out

    # the one standard way to say no: https://no-color.org
    monkeypatch.setenv("NO_COLOR", "1")
    emit_agent_event(_agent_event("finish", status="succeeded", turn=3, total_tokens=7955))
    assert "\x1b[" not in capsys.readouterr().out
