from typing import Type

from dlt.common.configuration import plugins

from dlt._workspace._workspace_context import is_workspace_active

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
        from dlt._workspace.cli._profile_command import ProfileCommand

        return ProfileCommand
    else:
        return None


@plugins.hookimpl(specname="plug_cli")
def plug_cli_workspace() -> Type[plugins.SupportsCliCommand]:
    if is_workspace_active():
        from dlt._workspace.cli._workspace_command import WorkspaceCommand

        return WorkspaceCommand
    else:
        return None
