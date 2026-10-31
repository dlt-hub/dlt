"""Plain-text transcript of an agent run, printed while it happens. Emojis mark its parts."""

import sys
from typing import Any, Callable, Dict, List

from dlt.common import json

from dlt._workspace.cli import echo as fmt
from dlt._workspace.deployment.agent.typing import TAgentEvent, TAgentEventKind

AGENT_RULE_WIDTH = 74
RULE_CHAR = "─"
DOT_SEP = " · "
DETAIL_CAPS = {0: 80, 1: 200}
"""How much of a tool argument, result or thought each verbosity shows. 2 shows all of it."""
AGENT_EMOJIS = {
    "system_prompt": "📜",
    "prompt": "📝",
    "thinks": "💭",
    "says": "💬",
    "tool": "🔧",
    "mcp": "🌐",
    "ok": "✅",
    "error": "❌",
    "result": "🎁",
    "finish": "🏁",
}
"""Emoji marking each part of an agent transcript. All are single code points shown two cells wide."""
STATUS_EMOJIS = {"succeeded": "✅", "failed": "❌", "aborted": "❗"}

TEventLines = Callable[[TAgentEvent, int, bool], List[str]]
"""Renders one event, at one verbosity, with or without emojis, as the lines it prints.
An empty string is a blank line."""


def mark(key: str, emojis: bool) -> str:
    """The emoji for `key` and a space, or nothing when emojis are off."""
    return f"{AGENT_EMOJIS[key]} " if emojis else ""


def status_mark(status: str, emojis: bool) -> str:
    """The emoji for an agent outcome and a space, or nothing when emojis are off."""
    return f"{STATUS_EMOJIS.get(status, AGENT_EMOJIS['finish'])} " if emojis else ""


def _width(text: str) -> int:
    """Console cells `text` takes: an emoji takes two."""
    emojis = set(AGENT_EMOJIS.values()) | set(STATUS_EMOJIS.values())
    return len(text) + sum(1 for ch in text if ch in emojis)


def _rule(label: str, tail: str = "") -> str:
    """`── label ──────── tail ──` filled to the console width."""
    fill = max(AGENT_RULE_WIDTH - _width(label) - (len(tail) + 4 if tail else 0) - 4, 2)
    rule = f"{RULE_CHAR * 2} {label} {RULE_CHAR * fill}"
    if tail:
        rule += f" {tail} {RULE_CHAR * 2}"
    return rule


def _excerpt(value: Any, verbosity: int) -> str:
    """One-line rendering of a detail, capped for the level."""
    text = value if isinstance(value, str) else json.dumps(value)
    text = " ".join(text.split())
    cap = DETAIL_CAPS.get(verbosity)
    return text if cap is None or len(text) <= cap else f"{text[:cap]}…"


def _indented(text: str, prefix: str = "  ") -> str:
    return "\n".join(f"{prefix}{line}" for line in text.strip().splitlines())


def _start_lines(event: TAgentEvent, verbosity: int, emojis: bool) -> List[str]:
    tail = DOT_SEP.join(filter(None, [event.get("model"), event.get("limits")]))
    return ["", _rule(event["agent"], tail)]


def _spoken_lines(key: str, label: str, keep_label: bool = True) -> TEventLines:
    """A label over the indented text: the prompt going in, the agent speaking."""

    def lines(event: TAgentEvent, verbosity: int, emojis: bool) -> List[str]:
        if emojis:
            # the emoji replaces the label unless `keep_label` is set
            header = f"{AGENT_EMOJIS[key]} {label}" if keep_label else AGENT_EMOJIS[key]
        else:
            header = label
        return ["", header, _indented(event.get("text", ""))]

    return lines


def _turn_lines(event: TAgentEvent, verbosity: int, emojis: bool) -> List[str]:
    tokens = ""
    if event.get("input_tokens") is not None:
        tokens = f"{event['input_tokens']:,} in / {event.get('output_tokens', 0):,} out"
    label = f"turn {event.get('turn', 0)}"
    pad = max(AGENT_RULE_WIDTH - len(label) - len(tokens), 1)
    return ["", f"{label}{' ' * pad}{tokens}"]


def _system_prompt_lines(event: TAgentEvent, verbosity: int, emojis: bool) -> List[str]:
    # the whole assembled prompt is long; only the most verbose level shows it
    if verbosity < 2:
        return []
    return _spoken_lines("system_prompt", "system prompt")(event, verbosity, emojis)


def _thinks_lines(event: TAgentEvent, verbosity: int, emojis: bool) -> List[str]:
    if verbosity == 0:
        return []
    marker = f"{mark('thinks', emojis)}thinks "
    return [f"  {marker} {_excerpt(event.get('text', ''), verbosity)}"]


def _tool_call_lines(event: TAgentEvent, verbosity: int, emojis: bool) -> List[str]:
    call = f"  {mark('mcp' if event.get('server') else 'tool', emojis)}{event.get('tool', 'tool')}"
    if event.get("server"):
        call += f" ({event['server']})"
    if verbosity > 0 and event.get("detail") is not None:
        call += f"  {_excerpt(event['detail'], verbosity)}"
    return [call]


def _tool_result_lines(event: TAgentEvent, verbosity: int, emojis: bool) -> List[str]:
    error = event.get("error")
    if emojis:
        marker = AGENT_EMOJIS["error" if error else "ok"]
    else:
        # without color or emoji, only the word tells a failed call apart
        marker = "→ error:" if error else "→"
    return [f"     {marker} {_excerpt(event.get('detail', ''), verbosity)}"]


def _mcp_lines(event: TAgentEvent, verbosity: int, emojis: bool) -> List[str]:
    marker = AGENT_EMOJIS["mcp"] if emojis else "mcp "
    return [f"  {marker} {event.get('text', '')}"]


def _finish_lines(event: TAgentEvent, verbosity: int, emojis: bool) -> List[str]:
    status = event.get("status", "finished")
    facts = [f"{event.get('turn', 0)} turns", f"{event.get('total_tokens', 0):,} tokens"]
    if event.get("cost_usd") is not None:
        facts.append(f"${event['cost_usd']:.2f}")
    lines = ["", _rule(status_mark(status, emojis) + status, DOT_SEP.join(facts))]
    used = [
        f"{label}: {', '.join(names)}"
        for label, names in (
            ("tools", event.get("tools")),
            ("skills", event.get("skills")),
            ("mcp tools", event.get("mcp_tools")),
        )
        if names
    ]
    if used:
        lines.append(f"  {DOT_SEP.join(used)}")
    return lines + [""]


EVENT_LINES: Dict[TAgentEventKind, TEventLines] = {
    "start": _start_lines,
    "system_prompt": _system_prompt_lines,
    "prompt": _spoken_lines("prompt", "prompt"),
    "turn": _turn_lines,
    "thinks": _thinks_lines,
    "says": _spoken_lines("says", "says", keep_label=False),
    "tool_call": _tool_call_lines,
    "tool_result": _tool_result_lines,
    "mcp": _mcp_lines,
    "finish": _finish_lines,
}


def emit_agent_event(event: TAgentEvent, verbosity: int = 1, emojis: bool = True) -> None:
    """Renders one step of an agent run as a plain-text transcript on stdout."""
    for line in EVENT_LINES[event["kind"]](event, verbosity, emojis):
        try:
            fmt.echo(line)
        except UnicodeEncodeError:
            # a stdout in a code page without emojis, e.g. a pipe on Windows: the run goes on
            encoding = getattr(sys.stdout, "encoding", None) or "ascii"
            fmt.echo(line.encode(encoding, "replace").decode(encoding))
