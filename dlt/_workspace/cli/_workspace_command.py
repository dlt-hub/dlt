import argparse

from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.pluggable_run_context import (
    PluggableRunContext,
    RunContextBase,
)

from dlt._workspace.cli import echo as fmt, utils
from dlt._workspace._workspace_context import WorkspaceRunContext
from dlt._workspace.cli.utils import check_delete_local_data, delete_local_data
from dlt._workspace.cli._pipeline_command import list_pipelines
from dlt._workspace.profile import read_profile_pin


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
    providers_context = Container()[PluggableRunContext].providers
    fmt.echo()
    fmt.echo("dlt found configuration in following locations:")
    total_not_found_count = 0
    for provider in providers_context.providers:
        fmt.echo("* %s" % fmt.bold(provider.name))
        for location in provider.present_locations:
            fmt.echo("    %s" % location)
        if provider.is_empty:
            fmt.echo("    provider is empty")
        # check for locations that were not found
        not_found_locations = set(provider.locations).difference(provider.present_locations)
        if not_found_locations:
            if verbosity > 0:
                # display details of not found locations
                for location in not_found_locations:
                    fmt.echo("    %s (not found)" % fmt.style(location, fg="yellow"))
            else:
                total_not_found_count += len(not_found_locations)
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
def start_mcp(run_context: WorkspaceRunContext, port: int, stdio: bool) -> None:
    from dlt._workspace.mcp import WorkspaceMCP

    transport = "stdio" if stdio else "sse"
    if transport:
        # write to stderr. stdin is the comm channel
        fmt.echo("Starting dlt MCP server", err=True)
    mcp_server = WorkspaceMCP(f"dlt: {run_context.name}@{run_context.profile}", port=port)
    mcp_server.run(transport)


@utils.track_command("dashboard", True)
def show_workspace(run_context: WorkspaceRunContext, edit: bool) -> None:
    from dlt._workspace.helpers.dashboard.runner import run_dashboard

    run_dashboard(edit=edit)
