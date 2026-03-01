import argparse

from dlt.common.configuration.specs.pluggable_run_context import (
    RunContextBase,
)

from dlt._workspace.cli import echo as fmt, utils
from dlt._workspace._workspace_context import WorkspaceRunContext
from dlt._workspace.typing import TLocationInfo
from dlt._workspace.cli.utils import (
    check_delete_local_data,
    delete_local_data,
    fetch_workspace_info,
)
from dlt._workspace.cli._pipeline_command import list_pipelines


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
        fmt.echo("Settings for profile %s:" % fmt.bold(profile["name"]))
        fmt.echo("  Pipelines and other working data: %s" % fmt.bold(profile["data_dir"]))
        fmt.echo("  Locally loaded data: %s" % fmt.bold(profile["local_dir"]))
        if profile["is_pinned"]:
            fmt.echo("  Profile is %s" % fmt.bold("pinned"))
        if profile["configured_profiles"]:
            fmt.echo(
                "Profiles with configs or pipelines: %s"
                % fmt.bold(", ".join(profile["configured_profiles"]))
            )

    fmt.echo()
    fmt.echo("dlt found configuration in following locations:")
    total_not_found_count = 0
    for prov in info["providers"]:
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


@utils.track_command("workspace", track_before=False, operation="clean")
def clean_workspace(run_context: RunContextBase, args: argparse.Namespace) -> None:
    fmt.echo("Local pipelines data will be removed. Remote destinations are not affected.")
    deleted_dirs = check_delete_local_data(run_context, args.skip_data_dir)
    if deleted_dirs:
        delete_local_data(run_context, deleted_dirs)


@utils.track_command("workspace", track_before=True, operation="mcp")
def start_mcp(run_context: WorkspaceRunContext, port: int, stdio: bool, sse: bool) -> None:
    from dlt._workspace.mcp import WorkspaceMCP

    if stdio:
        transport = "stdio"
    elif sse:
        transport = "sse"
    else:
        transport = "streamable-http"
    if transport != "stdio":
        fmt.echo("Starting dlt MCP server", err=True)
    mcp_server = WorkspaceMCP(f"dlt: {run_context.name}@{run_context.profile}", port=port)
    mcp_server.run(transport=transport)


@utils.track_command("dashboard", True)
def show_workspace(run_context: WorkspaceRunContext, edit: bool) -> None:
    from dlt._workspace.helpers.dashboard.runner import run_dashboard

    run_dashboard(edit=edit)
