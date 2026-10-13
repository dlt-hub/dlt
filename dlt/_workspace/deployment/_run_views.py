"""CLI views for `run` / `serve` orchestration: banner, warnings, plan, picker."""

import sys
from typing import Any, Dict, List, Optional, Sequence, Tuple

from dlt.common import json

from dlt._workspace.cli import echo as fmt
from dlt._workspace.deployment._job_ref import format_job_label
from dlt._workspace.deployment._run_typing import TRunBannerInfo, TRunJobInfo
from dlt._workspace.deployment.agent.transcript import mark, status_mark
from dlt._workspace.deployment.exceptions import AmbiguousJobSelector
from dlt._workspace.deployment.job_result import is_agent_result
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
    fmt.echo("job_ref: %s" % info["job_ref"])
    fmt.echo("trigger: %s" % info["trigger"])
    fmt.echo("launcher: %s" % info["launcher"])
    fmt.echo("run_id:  %s" % info["run_id"])
    fmt.echo("entry_point:")
    fmt.echo(json.typed_dumps(info["entry_point"], pretty=True))


def print_job_result(result: TJobResult, emojis: bool = True) -> None:
    """Render the structured result a job returned, after its run finished."""
    fields: Dict[str, Any] = dict(result)
    fmt.echo("")
    fmt.echo("%sResult  [%s]" % (mark("result", emojis), result["type"]))
    # agent results carry a status and a summary
    if is_agent_result(result["type"]):
        status = fields.get("status", "")
        fmt.echo("  status:     %s%s" % (status_mark(status, emojis), status))
        if summary := fields.get("summary"):
            fmt.echo("  summary:    %s" % summary)
    for entity in fields.get("object") or []:
        fmt.echo("  %s: %s" % (entity["type"], entity["id"]))
    if trace := fields.get("trace"):
        fmt.echo(
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
            fmt.echo("  local tools: %s" % (wired or "none"))
    payload = fields.get("result")
    if payload is not None:
        fmt.echo(json.typed_dumps(payload, pretty=True))


def print_run_banner(info: TRunBannerInfo) -> None:
    """Print the unified `Starting <job> [local|remote]` banner."""
    color = "green" if info["location"] == "local" else "cyan"
    chip = fmt.style(info["location"], fg=color)
    fmt.echo("Starting %s  [%s]" % (fmt.bold(info["display_label"]), chip))
    fmt.echo("  job_ref:    %s" % info["job_ref"])
    fmt.echo("  trigger:    %s" % info["trigger_humanized"])
    fmt.echo("  profile:    %s" % info["profile"])
    if "run_id" in info:
        fmt.echo("  run_id:     %s" % info["run_id"])
    if "workspace_name" in info:
        fmt.echo("  workspace:  %s" % info["workspace_name"])
    if "port" in info:
        fmt.echo("Listening on http://localhost:%d" % info["port"])


def pick_one_job(candidates: Sequence[TCandidate]) -> TCandidate:
    """Numbered interactive picker; raises `AmbiguousJobSelector` in non-tty contexts."""
    if not candidates:
        raise ValueError("pick_one_job called with empty candidate list")
    if len(candidates) == 1:
        return candidates[0]
    if not (sys.stdin.isatty() and sys.stdout.isatty()) or not fmt.is_interactive():
        raise AmbiguousJobSelector(candidates)

    fmt.echo("%d jobs match:" % len(candidates))
    for i, (jd, t) in enumerate(candidates, 1):
        label = format_job_label(jd["job_ref"], jd.get("expose"), jd.get("deliver"))
        fmt.echo("  %d. %s  (trigger: %s)" % (i, label, t))
    choice = fmt.prompt(
        "Pick a job",
        choices=[str(i) for i in range(1, len(candidates) + 1)],
        default="1",
    )
    return candidates[int(choice) - 1]
