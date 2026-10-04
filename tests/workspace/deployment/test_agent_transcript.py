"""Tests for the plain-text transcript printed while an agent runs."""

from typing import Any, List, Optional, cast

import pytest

from dlt._workspace.deployment.agent.transcript import (
    AGENT_EMOJIS,
    AGENT_RULE_WIDTH,
    emit_agent_event,
)
from dlt._workspace.deployment.agent.typing import TAgentEvent


def _agent_event(kind: str, **fields: Any) -> TAgentEvent:
    return cast(TAgentEvent, {"kind": kind, "agent": "job-inspector", **fields})


TRANSCRIPT_EMOJIS = [
    "\n── job-inspector ",
    "📝 prompt\n  Investigate run '89826ee6'.",
    "🌐 dlt-workspace-mcp connected",
    "\nturn 1",
    "💭 thinks  the run id is there",
    "🌐 list_runs (dlt-workspace-mcp)",
    "✅ 3 runs",
    "💬\n  The cursor produced duplicates.",
    "── ✅ succeeded ",
    "\n  tools: Read · mcp tools: list_runs",
]
TRANSCRIPT_PLAIN = [
    "── job-inspector ",
    "prompt\n  Investigate run '89826ee6'.",
    "mcp  dlt-workspace-mcp connected",
    "turn 1",
    "thinks  the run id is there",
    "  list_runs (dlt-workspace-mcp)",
    "→ 3 runs",
    "says\n  The cursor produced duplicates.",
    "── succeeded ",
    "  tools: Read · mcp tools: list_runs",
]


@pytest.mark.parametrize(
    "emojis,expected",
    [(True, TRANSCRIPT_EMOJIS), (False, TRANSCRIPT_PLAIN)],
    ids=["emojis", "plain"],
)
def test_emit_agent_event_renders_the_transcript(
    capsys: pytest.CaptureFixture[str], emojis: bool, expected: List[str]
) -> None:
    """One assertion per kind: what a person watching the run must see. Never any color."""
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
        emit_agent_event(event, emojis=emojis)
    out = capsys.readouterr().out

    assert "\x1b[" not in out
    for line in expected:
        assert line in out
    assert "claude-sonnet-5 · max 30 turns" in out and "1,204 in / 340 out" in out
    assert '{"n":3}' in out and "3 turns · 7,955 tokens · $0.04" in out
    if not emojis:
        assert not set(out) & set("".join(AGENT_EMOJIS.values()))


@pytest.mark.parametrize(
    "verbosity,thinking,detail",
    [(0, False, 80), (1, True, 200), (2, True, None)],
    ids=["quiet", "default", "everything"],
)
def test_agent_verbosity_caps_thinking_and_detail(
    capsys: pytest.CaptureFixture[str], verbosity: int, thinking: bool, detail: Optional[int]
) -> None:
    emit_agent_event(_agent_event("thinks", text="t" * 500), verbosity)
    emit_agent_event(_agent_event("tool_result", tool="read", detail="r" * 500), verbosity)
    out = capsys.readouterr().out

    assert ("t" * 20 in out) is thinking
    assert ("r" * 500 in out) is (detail is None)
    if detail is not None:
        assert f"{'r' * detail}…" in out


@pytest.mark.parametrize(
    "emojis,failed,ok",
    [(True, "❌ boom", "✅ fine"), (False, "→ error: boom", "→ fine")],
    ids=["emojis", "plain"],
)
def test_a_failing_tool_result_says_so_without_color(
    capsys: pytest.CaptureFixture[str], emojis: bool, failed: str, ok: str
) -> None:
    emit_agent_event(
        _agent_event("tool_result", tool="run_bash", detail="boom", error=True), emojis=emojis
    )
    emit_agent_event(_agent_event("tool_result", tool="run_bash", detail="fine"), emojis=emojis)
    failed_line, ok_line = capsys.readouterr().out.splitlines()

    assert failed_line.strip() == failed
    assert ok_line.strip() == ok


@pytest.mark.parametrize(
    "status,mark",
    [("succeeded", "✅"), ("failed", "❌"), ("aborted", "❗"), ("timeout", "🏁")],
    ids=["succeeded", "failed", "aborted", "other"],
)
def test_finish_marks_the_status(
    capsys: pytest.CaptureFixture[str], status: str, mark: str
) -> None:
    emit_agent_event(_agent_event("finish", status=status, turn=1, total_tokens=10))
    rule = capsys.readouterr().out.splitlines()[1]

    assert rule.startswith(f"── {mark} {status} ")
    # an emoji takes two cells: the rule ends where one without it does
    assert len(rule) + 1 == AGENT_RULE_WIDTH
