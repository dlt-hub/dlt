import argparse
import sys
from typing import Dict, List, Optional, Tuple

from dlt.common import json
from dlt.common.configuration.specs.pluggable_run_context import RunContextBase

from dlt._workspace._workspace_context import WorkspaceRunContext
from dlt._workspace.cli import echo as fmt, utils
from dlt._workspace.cli._pipeline_command import list_pipelines
from dlt._workspace.cli.dlthub.typing import (
    TCurrentProfileInfo,
    TDeploymentManifestInfo,
)
from dlt._workspace.cli.dlthub.utils import (
    check_delete_local_data,
    delete_local_data,
    fetch_deployment_info,
    fetch_workspace_info,
)
from dlt._workspace.typing import TLocationInfo, TProviderInfo


def _format_location_tag(loc: TLocationInfo) -> str:
    """Build a human-readable tag for a location (e.g. '(global, profile: dev)')."""
    parts = []
    if loc["scope"] == "global":
        parts.append("global")
    if profile_name := loc.get("profile_name"):
        parts.append("profile: %s" % profile_name)
    if not loc["present"]:
        parts.append("not found")
    if parts:
        return " (%s)" % ", ".join(parts)
    return ""


def print_profile_section(
    profile: TCurrentProfileInfo, configured_profiles: Optional[List[str]] = None
) -> None:
    """Renders the active-profile block: name, paths, pinned status, configured profiles."""
    fmt.echo("Settings for profile %s:" % fmt.bold(profile["name"]))
    fmt.echo("  Pipelines and other working data: %s" % fmt.bold(profile["data_dir"]))
    fmt.echo("  Locally loaded data: %s" % fmt.bold(profile["local_dir"]))
    if profile["is_pinned"]:
        fmt.echo("  Profile is %s" % fmt.bold("pinned"))
    if configured_profiles:
        fmt.echo(
            "Profiles with configs or pipelines: %s" % fmt.bold(", ".join(configured_profiles))
        )


def print_providers(providers: List[TProviderInfo], verbosity: int) -> None:
    """Renders configuration provider locations; verbosity > 0 also lists not-found locations."""
    fmt.echo(fmt.cli_cmd("found configuration in following locations:"))
    total_not_found_count = 0
    for prov in providers:
        fmt.echo("* %s" % fmt.bold(prov["name"]))
        for loc in prov["locations"]:
            if loc["present"]:
                tag = _format_location_tag(loc)
                fmt.echo("    %s%s" % (loc["path"], tag))
            else:
                if verbosity > 0:
                    tag = _format_location_tag(loc)
                    fmt.echo("    %s" % fmt.style("%s%s" % (loc["path"], tag), fg="yellow"))
                else:
                    total_not_found_count += 1
        if prov["is_empty"]:
            fmt.echo("    provider is empty")
    if verbosity == 0 and total_not_found_count > 0:
        fmt.echo(
            "%s location(s) were probed but not found. Use %s to see details."
            % (fmt.bold(str(total_not_found_count)), fmt.bold("-v"))
        )


@utils.track_command("workspace", track_before=False, operation="info")
def print_workspace_info(run_context: WorkspaceRunContext, verbosity: int = 0) -> None:
    info = fetch_workspace_info()

    if info["name"]:
        fmt.echo("Workspace %s:" % fmt.bold(info["name"]))
    fmt.echo("Workspace dir: %s" % fmt.bold(info["run_dir"]))
    fmt.echo("Settings dir: %s" % fmt.bold(info["settings_dir"]))

    profile = info["profile"]
    if profile:
        fmt.echo()
        print_profile_section(profile, info["configured_profiles"])

    fmt.echo()
    print_providers(info["providers"], verbosity)
    # installed toolkits
    if info["installed_toolkits"]:
        fmt.echo()
        fmt.echo(
            "Installed toolkits: %s"
            % ", ".join(fmt.bold(n) for n in sorted(info["installed_toolkits"]))
        )

    # list pipelines in the workspace
    fmt.echo()
    list_pipelines(run_context.get_data_entity("pipelines"), verbosity)

    # deployment manifest summary
    fmt.echo()
    _print_deployment_info(fetch_deployment_info(), verbosity)


def _print_deployment_info(info: TDeploymentManifestInfo, verbosity: int) -> None:
    status = info["status"]
    if status == "not_found":
        fmt.echo("Deployment: no manifest found (create __deployment__.py)")
        return
    if status == "generation_failed":
        fmt.warning("Deployment: manifest generation failed")
        if verbosity > 0 and info.get("error"):
            fmt.echo(info["error"])
        return

    counts = info["counts_by_category"]
    total = info["total_jobs"]
    # sort by descending count, then by category name for stability
    parts = [
        "%d %s(s)" % (n, cat) for cat, n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
    ]
    breakdown = ", ".join(parts)
    fmt.echo("Deployment: %s job(s): %s" % (fmt.bold(str(total)), breakdown))

    if verbosity == 0:
        return

    max_name = max((len(j["display_label"]) for j in info["jobs"]), default=8)
    for job in info["jobs"]:
        triggers: List[str] = []
        if "default_trigger" in job:
            triggers.append("🎯 %s" % job["default_trigger"])
        triggers.extend(job["triggers"])
        if not triggers:
            if job["category"] in ("interactive", "mcp", "dashboard", "notebook"):
                triggers = ["(interactive)"]
            else:
                triggers = ["(manual only)"]
        fmt.echo("  %s  %s" % (job["display_label"].ljust(max_name), ", ".join(triggers)))


@utils.track_command("workspace", track_before=False, operation="clean")
def clean_workspace(run_context: RunContextBase, args: argparse.Namespace) -> None:
    fmt.echo("Local pipelines data will be removed. Remote destinations are not affected.")
    deleted_dirs = check_delete_local_data(run_context, args.skip_data_dir)
    if deleted_dirs:
        delete_local_data(run_context, deleted_dirs)


@utils.track_command("dashboard", True)
def show_workspace(run_context: WorkspaceRunContext, edit: bool) -> None:
    from dlt._workspace.helpers.dashboard.runner import run_dashboard

    run_dashboard(edit=edit)


def _add_pipeline_name(parser: argparse.ArgumentParser, _op: str) -> None:
    parser.add_argument("pipeline_name", nargs="?", help="Pipeline name")


def _parse_config_args(pairs: List[str]) -> Dict[str, str]:
    config: Dict[str, str] = {}
    for pair in pairs:
        if "=" not in pair:
            raise ValueError(f"config must be KEY=VALUE, got: {pair!r}")
        key, value = pair.split("=", 1)
        config[key] = value
    return config


def execute_run(args: argparse.Namespace) -> None:
    """Resolve and launch a single workspace job for `dlthub local run`."""
    from dlt._workspace.cli.dlthub._run_command import (
        fetch_run_info,
        print_run_plan,
        print_run_starting,
        print_run_warnings,
    )
    from dlt._workspace.deployment._job_ref import format_job_label
    from dlt._workspace.deployment.launchers._launcher import exec_process
    from dlt._workspace.deployment.typing import TJobDefinition, TTrigger

    def _pick(
        candidates: List[Tuple["TJobDefinition", "TTrigger"]],
    ) -> Tuple["TJobDefinition", "TTrigger"]:
        if len(candidates) == 1:
            return candidates[0]

        def _label(j: "TJobDefinition") -> str:
            return format_job_label(j["job_ref"], j.get("expose"), j.get("deliver"))

        labels = [f"{i}-{_label(j)}" for i, (j, _) in enumerate(candidates, 1)]
        fmt.echo(f"{len(candidates)} jobs match:")
        for i, (j, t) in enumerate(candidates, 1):
            fmt.echo(f"  {i}. {_label(j)}  (trigger: {t})")
        choice = fmt.prompt(
            "Pick a job: " + ", ".join(labels),
            choices=[str(i) for i in range(1, len(candidates) + 1)],
            default="1",
        )
        return candidates[int(choice) - 1]

    cli_config = _parse_config_args(args.config) if args.config else {}
    info = fetch_run_info(
        selector=args.selector_or_job_ref,
        file=args.file,
        user_profile=args.profile,
        user_start=args.start,
        user_end=args.end,
        user_refresh=args.refresh,
        cli_config=cli_config,
        pick=_pick,
    )
    if info is None:
        fmt.echo("No jobs found in manifest.")
        return

    print_run_warnings(info)

    if getattr(args, "verbosity", 0) or args.dry_run:
        print_run_plan(info)
    if args.dry_run:
        fmt.echo("--dry-run: not launching")
        return

    print_run_starting(info)
    exec_process(
        [
            sys.executable,
            "-u",
            "-m",
            info["launcher"],
            "--run-id",
            info["run_id"],
            "--trigger",
            info["trigger"],
            "--entry-point",
            json.typed_dumps(info["entry_point"]),
        ]
    )
