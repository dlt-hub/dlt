import argparse

from dlt.common.configuration.specs.pluggable_run_context import (
    RunContextBase,
)

from dlt._workspace.cli import echo as fmt, utils
from dlt._workspace._workspace_context import WorkspaceRunContext
from dlt._workspace.cli.utils import check_delete_local_data, delete_local_data
from dlt._workspace.utils import ProviderLocationInfo, get_provider_locations
from dlt._workspace.cli._pipeline_command import list_pipelines
from dlt._workspace.profile import read_profile_pin


def _format_location_tag(loc: ProviderLocationInfo) -> str:
    """Build a human-readable tag for a location (e.g. '(global, profile: dev)')."""
    parts = []
    if loc.scope == "global":
        parts.append("global")
    if loc.profile_name:
        parts.append("profile: %s" % loc.profile_name)
    if not loc.present:
        parts.append("not found")
    if parts:
        return " (%s)" % ", ".join(parts)
    return ""


@utils.track_command("workspace", track_before=False, operation="info")
def print_workspace_info(run_context: WorkspaceRunContext, verbosity: int = 0) -> None:
    fmt.echo("Workspace %s:" % fmt.bold(run_context.name))
    fmt.echo("Workspace dir: %s" % fmt.bold(run_context.run_dir))
    fmt.echo("Settings dir: %s" % fmt.bold(run_context.settings_dir))
    # profile info
    fmt.echo()
    fmt.echo("Settings for profile %s:" % fmt.bold(run_context.profile))
    fmt.echo("  Pipelines and other working data: %s" % fmt.bold(run_context.data_dir))
    fmt.echo("  Locally loaded data: %s" % fmt.bold(run_context.local_dir))
    if run_context.profile == read_profile_pin(run_context):
        fmt.echo("  Profile is %s" % fmt.bold("pinned"))
    configured_profiles = run_context.configured_profiles()
    if configured_profiles:
        fmt.echo(
            "Profiles with configs or pipelines: %s" % fmt.bold(", ".join(configured_profiles))
        )

    # provider info
    fmt.echo()
    fmt.echo("dlt found configuration in following locations:")
    total_not_found_count = 0
    for info in get_provider_locations():
        provider = info.provider
        fmt.echo("* %s" % fmt.bold(provider.name))
        for loc in info.locations:
            if loc.present:
                tag = _format_location_tag(loc)
                fmt.echo("    %s%s" % (loc.path, tag))
            else:
                if verbosity > 0:
                    tag = _format_location_tag(loc)
                    fmt.echo("    %s" % fmt.style("%s%s" % (loc.path, tag), fg="yellow"))
                else:
                    total_not_found_count += 1
        if provider.is_empty:
            fmt.echo("    provider is empty")
    # at verbosity 0, show summary of not found locations
    if verbosity == 0 and total_not_found_count > 0:
        fmt.echo(
            "%s location(s) were probed but not found. Use %s to see details."
            % (fmt.bold(str(total_not_found_count)), fmt.bold("-v"))
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
