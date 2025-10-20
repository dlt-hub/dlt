import argparse

from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.pluggable_run_context import (
    PluggableRunContext,
    RunContextBase,
)

from dlt._workspace.cli import SupportsCliCommand, echo as fmt, utils
from dlt._workspace._workspace_context import WorkspaceRunContext, active
from dlt._workspace.cli.utils import add_mcp_arg_parser, delete_local_data
from dlt._workspace.profile import read_profile_pin


class WorkspaceCommand(SupportsCliCommand):
    command = "workspace"
    help_string = "Manage current Workspace"
    description = """
Commands to get info, cleanup local files and launch Workspace MCP
"""

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
        self.parser = parser

        subparsers = parser.add_subparsers(
            title="Available subcommands", dest="workspace_command", required=False
        )

        # clean command
        clean_local_parser = subparsers.add_parser(
            "clean",
            help=(
                "Cleans local data for the selected profile. Locally loaded data will be deleted. "
                "Pipelines working directories are also deleted by default. Data in remote "
                "destinations is not affected."
            ),
        )
        clean_local_parser.add_argument(
            "--skip-data-dir",
            action="store_true",
            default=False,
            help="Do not delete pipelines working dir.",
        )

        subparsers.add_parser(
            "info",
            help="Displays workspace info.",
        )

        DEFAULT_DLT_MCP_PORT = 43654
        add_mcp_arg_parser(
            subparsers,
            "This MCP allows to attach to any pipeline that was previously ran in this workspace"
            " and then facilitates schema and data exploration in the pipeline's dataset.",
            "Launch dlt MCP server in current Python environment and Workspace in SSE transport"
            " mode",
            DEFAULT_DLT_MCP_PORT,
        )

    def execute(self, args: argparse.Namespace) -> None:
        workspace_context = active()

        if args.workspace_command == "info" or not args.workspace_command:
            print_workspace_info(workspace_context)
        elif args.workspace_command == "clean":
            clean_workspace(workspace_context, args)
        elif args.workspace_command == "mcp":
            start_mcp(workspace_context, port=args.port, stdio=args.stdio)
        else:
            self.parser.print_usage()


def print_workspace_info(run_context: WorkspaceRunContext) -> None:
    # fmt.echo("Workspace %s:" % fmt.bold(run_context.name))
    fmt.echo("Workspace dir: %s" % fmt.bold(run_context.run_dir))
    fmt.echo("Settings dir: %s" % fmt.bold(run_context.settings_dir))
    # profile info
    fmt.echo()
    fmt.echo("Settings for profile %s:" % fmt.bold(run_context.profile))
    fmt.echo("  Pipelines and other working data: %s" % fmt.bold(run_context.data_dir))
    fmt.echo("  Locally loaded data: %s" % fmt.bold(run_context.local_dir))
    if run_context.profile == read_profile_pin(run_context):
        fmt.echo("  Profile in %s" % fmt.bold("pinned"))
    # provider info
    providers_context = Container()[PluggableRunContext].providers
    fmt.echo()
    fmt.echo("dlt reads configuration from following locations:")
    for provider in providers_context.providers:
        fmt.echo("* %s" % fmt.bold(provider.name))
        for location in provider.locations:
            fmt.echo("    %s" % location)
        if provider.is_empty:
            fmt.echo("    provider is empty")


def clean_workspace(run_context: RunContextBase, args: argparse.Namespace) -> None:
    delete_local_data(run_context, args.skip_data_dir)


@utils.track_command("mcp_run", track_before=True)
def start_mcp(run_context: WorkspaceRunContext, port: int, stdio: bool) -> None:
    from dlt._workspace.mcp import WorkspaceMCP

    transport = "stdio" if stdio else "sse"
    if transport:
        # write to stderr. stdin is the comm channel
        fmt.echo("Starting dlt MCP server", err=True)
    mcp_server = WorkspaceMCP(f"dlt: {run_context.name}@{run_context.profile}", port=port)
    mcp_server.run(transport)
