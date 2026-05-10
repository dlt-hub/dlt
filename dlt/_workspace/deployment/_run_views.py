"""CLI views for `run` / `serve` orchestration: banner, warnings, plan, picker."""

import sys
from typing import List, Optional, Sequence, Tuple

from dlt.common import json

from dlt._workspace.cli import echo as fmt
from dlt._workspace.deployment._job_ref import format_job_label
from dlt._workspace.deployment._run_typing import TRunBannerInfo, TRunJobInfo
from dlt._workspace.deployment.exceptions import AmbiguousJobSelector
from dlt._workspace.deployment.typing import TJobDefinition


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
