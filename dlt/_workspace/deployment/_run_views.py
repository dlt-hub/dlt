"""CLI views for `run` / `serve` orchestration: banner, warnings, plan, picker."""

import os
import sys
from typing import Any, Dict, List, Optional, Sequence, Tuple, cast

from dlt.common import json

from dlt._workspace.cli import echo as fmt
from dlt._workspace.deployment._job_ref import format_job_label
from dlt._workspace.deployment._run_typing import TAgentEvent, TRunBannerInfo, TRunJobInfo
from dlt._workspace.deployment.exceptions import AmbiguousJobSelector
from dlt._workspace.deployment.job_result import parse_result_type
from dlt._workspace.deployment.typing import TJobDefinition, TJobResult


TCandidate = Tuple[TJobDefinition, str]


def print_run_warnings(
    warnings: List[str],
    *,
    refresh_warning: Optional[str] = None,
    profile_warning: Optional[str] = None,
) -> None:
    """Emit each manifest/refresh/profile warning via `fmt.warning`."""
    for w in warnings:
        fmt.warning(w)
    if refresh_warning:
        fmt.warning(refresh_warning)
    if profile_warning:
        fmt.warning(profile_warning)


def print_run_plan(info: TRunJobInfo) -> None:
    """Render the resolved run plan (used for `-v` / `--dry-run`)."""
    _echo("job_ref: %s" % info["job_ref"])
    _echo("trigger: %s" % info["trigger"])
    _echo("launcher: %s" % info["launcher"])
    _echo("run_id:  %s" % info["run_id"])
    _echo("entry_point:")
    _echo(json.typed_dumps(info["entry_point"], pretty=True))


def print_job_result(result: TJobResult) -> None:
    """Render the structured result a job returned, after its run finished."""
    fields: Dict[str, Any] = dict(result)
    category, _ = parse_result_type(result["type"])
    _echo("")
    _echo("Result  [%s]" % fmt.style(result["type"], fg="cyan"))
    # the category says an agent ran, and so that status, summary and trace are there
    if category == "background_agent":
        status = fields.get("status", "")
        color = {"succeeded": "green", "failed": "red", "aborted": "yellow"}.get(status, "white")
        _echo("  status:     %s" % fmt.style(status, fg=color))
        if summary := fields.get("summary"):
            _echo("  summary:    %s" % summary)
    for entity in fields.get("object") or []:
        _echo("  %s: %s" % (entity["type"], entity["id"]))
    if trace := fields.get("trace"):
        _echo(
            "  loop:       %s on %s, %s turns, %s tokens"
            % (
                trace.get("loop_type", "?"),
                trace.get("model", "?"),
                trace.get("turn_count", 0),
                trace.get("total_tokens", 0),
            )
        )
        if (tools := trace.get("local_tools")) is not None:
            wired = ", ".join(f"{name} ({verb})" for name, verb in tools.items())
            _echo("  local tools: %s" % (wired or "none"))
    payload = fields.get("result")
    if payload is not None:
        _echo(json.typed_dumps(payload, pretty=True))


AGENT_RULE_WIDTH = 74
DETAIL_CAPS = {0: 80, 1: 200}
"""How much of a tool argument, result or thought each verbosity shows. 2 shows all of it."""


def _echo(text: str = "") -> None:
    """Writes a line with its colors, terminal or not. `NO_COLOR` turns them off."""
    # a runner's log viewer renders ANSI, and click would strip it for anything but a terminal
    fmt.echo(text, color=not os.environ.get("NO_COLOR"))


def _rule(label: str, tail: str = "", **label_style: Any) -> str:
    """`── label ──────── tail ──` filled to the console width, the label styled."""
    fill = max(AGENT_RULE_WIDTH - len(label) - (len(tail) + 4 if tail else 0) - 4, 2)
    rule = fmt.style("\u2500\u2500 ", dim=True) + fmt.style(label, **label_style)
    rule += fmt.style(f" {'\u2500' * fill}", dim=True)
    if tail:
        rule += fmt.style(f" {tail} \u2500\u2500", dim=True)
    return rule


def _excerpt(value: Any, verbosity: int) -> str:
    """One-line rendering of a detail, capped for the level."""
    text = value if isinstance(value, str) else json.dumps(value)
    text = " ".join(text.split())
    cap = DETAIL_CAPS.get(verbosity)
    return text if cap is None or len(text) <= cap else f"{text[:cap]}\u2026"


def _indented(text: str, prefix: str = "  ") -> str:
    return "\n".join(f"{prefix}{line}" for line in text.strip().splitlines())


def print_agent_event(event: TAgentEvent, verbosity: int = 1) -> None:
    """Render one step of an agent run as a transcript."""
    kind = event["kind"]

    if kind == "start":
        tail = " \u00b7 ".join(filter(None, [event.get("model"), event.get("limits")]))
        _echo("")
        _echo(_rule(event["agent"], tail, bold=True))
    elif kind == "prompt":
        _echo("")
        _echo(fmt.style("prompt", fg="cyan"))
        _echo(_indented(event.get("text", "")))
    elif kind == "turn":
        tokens = ""
        if event.get("input_tokens") is not None:
            tokens = f"{event['input_tokens']:,} in / {event.get('output_tokens', 0):,} out"
        label = f"turn {event.get('turn', 0)}"
        pad = max(AGENT_RULE_WIDTH - len(label) - len(tokens), 1)
        _echo("")
        _echo(fmt.style(f"{label}{' ' * pad}{tokens}", dim=True))
    elif kind == "thinks":
        if verbosity > 0:
            _echo(fmt.style(f"  thinks  {_excerpt(event.get('text', ''), verbosity)}", dim=True))
    elif kind == "says":
        _echo("")
        _echo(fmt.style("says", fg="cyan"))
        _echo(_indented(event.get("text", "")))
    elif kind == "tool_call":
        call = f"  {fmt.style(event.get('tool', 'tool'), fg='yellow')}"
        if event.get("server"):
            call += fmt.style(f" ({event['server']})", dim=True)
        if verbosity > 0 and event.get("detail") is not None:
            call += f"  {fmt.style(_excerpt(event['detail'], verbosity), dim=True)}"
        _echo(call)
    elif kind == "tool_result":
        color = "red" if event.get("error") else "green"
        _echo(
            f"     {fmt.style('\u2192', fg=color)}"
            f" {fmt.style(_excerpt(event.get('detail', ''), verbosity), dim=True)}"
        )
    elif kind == "mcp":
        _echo(fmt.style(f"  mcp  {event.get('text', '')}", dim=True))
    elif kind == "finish":
        status = event.get("status", "finished")
        color = {"succeeded": "green", "failed": "red", "aborted": "yellow"}.get(status, "white")
        facts = [f"{event.get('turn', 0)} turns", f"{event.get('total_tokens', 0):,} tokens"]
        if event.get("cost_usd") is not None:
            facts.append(f"${event['cost_usd']:.2f}")
        _echo("")
        _echo(_rule(status, " \u00b7 ".join(facts), fg=color))
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
            _echo(fmt.style(f"  {' \u00b7 '.join(used)}", dim=True))
        _echo("")


def emit_agent_event(event: TAgentEvent, verbosity: int = 1) -> None:
    """Shows a run step on stdout, whether or not a terminal is attached."""
    print_agent_event(event, verbosity)


def print_run_banner(info: TRunBannerInfo) -> None:
    """Print the unified `Starting <job> [local|remote]` banner."""
    color = "green" if info["location"] == "local" else "cyan"
    chip = fmt.style(info["location"], fg=color)
    _echo("Starting %s  [%s]" % (fmt.bold(info["display_label"]), chip))
    _echo("  job_ref:    %s" % info["job_ref"])
    _echo("  trigger:    %s" % info["trigger_humanized"])
    _echo("  profile:    %s" % info["profile"])
    if "run_id" in info:
        _echo("  run_id:     %s" % info["run_id"])
    if "workspace_name" in info:
        _echo("  workspace:  %s" % info["workspace_name"])
    if "port" in info:
        _echo("Listening on http://localhost:%d" % info["port"])


def pick_one_job(candidates: Sequence[TCandidate]) -> TCandidate:
    """Numbered interactive picker; raises `AmbiguousJobSelector` in non-tty contexts."""
    if not candidates:
        raise ValueError("pick_one_job called with empty candidate list")
    if len(candidates) == 1:
        return candidates[0]
    if not (sys.stdin.isatty() and sys.stdout.isatty()) or not fmt.is_interactive():
        raise AmbiguousJobSelector(candidates)

    _echo("%d jobs match:" % len(candidates))
    for i, (jd, t) in enumerate(candidates, 1):
        label = format_job_label(jd["job_ref"], jd.get("expose"), jd.get("deliver"))
        _echo("  %d. %s  (trigger: %s)" % (i, label, t))
    choice = fmt.prompt(
        "Pick a job",
        choices=[str(i) for i in range(1, len(candidates) + 1)],
        default="1",
    )
    return candidates[int(choice) - 1]
