"""Module registering command line plugins
To add a new plugin here, do the following:
1. create a new command class in like ie. `dlt._workspace.cli.commands `ProfileCommand(SupportsCliCommand):`
2. provide the implementation of command functions like ie. in `dlt._workspace.cli._profile_command`
3. remember to wrap command in telemetry ie. @utils.track_command("profile", track_before=False, operation="info")
4. register the plugin here.

this module is inspected by pluggy on dlt startup
"""
from typing import Type

from dlt.common.configuration import plugins
from dlt.common.runtime.run_context import active as run_context_active


__all__ = [
    "plug_cli_init",
    "plug_cli_pipeline",
    "plug_cli_schema",
    "plug_cli_dashboard",
    "plug_cli_telemetry",
    "plug_cli_deploy",
    "plug_cli_docs",
    "plug_cli_ai",
    "plug_cli_profile",
    "plug_cli_workspace",
]


def is_workspace_active() -> bool:
    # verify run context type without importing

    ctx = run_context_active()
    return ctx.__class__.__name__ == "WorkspaceRunContext"


@plugins.hookimpl(specname="plug_cli")
def plug_cli_init() -> Type[plugins.SupportsCliCommand]:
    from dlt._workspace.cli.commands import InitCommand

    return InitCommand


@plugins.hookimpl(specname="plug_cli")
def plug_cli_pipeline() -> Type[plugins.SupportsCliCommand]:
    from dlt._workspace.cli.commands import PipelineCommand

    return PipelineCommand


@plugins.hookimpl(specname="plug_cli")
def plug_cli_schema() -> Type[plugins.SupportsCliCommand]:
    from dlt._workspace.cli.commands import SchemaCommand

    return SchemaCommand


@plugins.hookimpl(specname="plug_cli")
def plug_cli_dashboard() -> Type[plugins.SupportsCliCommand]:
    from dlt._workspace.cli.commands import DashboardCommand

    return DashboardCommand


@plugins.hookimpl(specname="plug_cli")
def plug_cli_telemetry() -> Type[plugins.SupportsCliCommand]:
    from dlt._workspace.cli.commands import TelemetryCommand

    return TelemetryCommand


@plugins.hookimpl(specname="plug_cli")
def plug_cli_deploy() -> Type[plugins.SupportsCliCommand]:
    from dlt._workspace.cli.commands import DeployCommand

    return DeployCommand


@plugins.hookimpl(specname="plug_cli")
def plug_cli_docs() -> Type[plugins.SupportsCliCommand]:
    from dlt._workspace.cli.commands import CliDocsCommand

    return CliDocsCommand


@plugins.hookimpl(specname="plug_cli")
def plug_cli_ai() -> Type[plugins.SupportsCliCommand]:
    from dlt._workspace.cli.commands import AiCommand

    return AiCommand


@plugins.hookimpl(specname="plug_cli")
def plug_cli_profile() -> Type[plugins.SupportsCliCommand]:
    if is_workspace_active():
        from dlt._workspace.cli.commands import ProfileCommand

        return ProfileCommand
    else:
        return None


@plugins.hookimpl(specname="plug_cli")
def plug_cli_workspace() -> Type[plugins.SupportsCliCommand]:
    if is_workspace_active():
        from dlt._workspace.cli.commands import WorkspaceCommand

        return WorkspaceCommand
    else:
        return None
