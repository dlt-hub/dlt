"""Module registering command line plugins
To add a new plugin here, do the following:
1. create a new command class in like ie. `dlt._workspace.cli.dlthub.commands.ProfileCommand(SupportsCliCommand):`
2. provide the implementation of command functions like ie. in `dlt._workspace.cli.dlthub._profile_command`
3. wrap command in telemetry. Single-host: decorate the controller, e.g.
   `@utils.track_command("profile", False, operation="info")`. Cross-host (same controller
   reached from both `dlt` and `dlthub`): leave it undecorated and wrap at the dispatch site,
   e.g. `utils.track_command("local.pipeline", True, "operation")(fn)(...)`, so each path
   emits its own event name. Active host is auto-attached as `host`.
4. register the plugin here.
5. no more imports in this module

this module is inspected by pluggy on dlt startup
"""
from typing import Optional, Type

from dlt.common.configuration import plugins
from dlt.common.configuration.plugins import only_host
from dlt.common.runtime.run_context import active as run_context_active


__all__ = [
    "plug_cli_init",
    "plug_cli_pipeline",
    "plug_cli_schema",
    "plug_cli_dashboard",
    "plug_cli_telemetry",
    "plug_cli_deploy",
    "plug_cli_ai_moved",
    "plug_cli_ai",
    "plug_cli_dlthub_init",
    "plug_cli_dlthub_pipeline",
    "plug_cli_dlthub_local",
    "plug_cli_dlthub_profile",
]


def is_workspace_active() -> bool:
    # verify run context type without importing
    ctx = run_context_active()
    return ctx.__class__.__name__ == "WorkspaceRunContext"


@plugins.hookimpl(specname="plug_cli")
@only_host("dlt")
def plug_cli_init(host: str) -> Optional[Type[plugins.SupportsCliCommand]]:
    from dlt._workspace.cli.commands import InitCommand

    return InitCommand


@plugins.hookimpl(specname="plug_cli")
@only_host("dlt")
def plug_cli_pipeline(host: str) -> Optional[Type[plugins.SupportsCliCommand]]:
    from dlt._workspace.cli.commands import PipelineCommand

    return PipelineCommand


@plugins.hookimpl(specname="plug_cli")
@only_host("dlt")
def plug_cli_schema(host: str) -> Optional[Type[plugins.SupportsCliCommand]]:
    # dlt-only at the top level. The dlthub host hosts schema as `dlthub local schema`.
    from dlt._workspace.cli.commands import SchemaCommand

    return SchemaCommand


@plugins.hookimpl(specname="plug_cli")
@only_host("dlt")
def plug_cli_dashboard(host: str) -> Optional[Type[plugins.SupportsCliCommand]]:
    from dlt._workspace.cli.commands import DashboardCommand

    return DashboardCommand


@plugins.hookimpl(specname="plug_cli")
@only_host("dlt")
def plug_cli_telemetry(host: str) -> Optional[Type[plugins.SupportsCliCommand]]:
    # dlt-only at the top level. The dlthub host hosts telemetry as `dlthub local telemetry`.
    from dlt._workspace.cli.commands import TelemetryCommand

    return TelemetryCommand


@plugins.hookimpl(specname="plug_cli")
@only_host("dlt")
def plug_cli_deploy(host: str) -> Optional[Type[plugins.SupportsCliCommand]]:
    from dlt._workspace.cli.commands import DeployCommand

    return DeployCommand


@plugins.hookimpl(specname="plug_cli")
@only_host("dlt")
def plug_cli_ai_moved(host: str) -> Optional[Type[plugins.SupportsCliCommand]]:
    from dlt._workspace.cli.commands import make_moved_to_dlthub_command

    return make_moved_to_dlthub_command("ai", "ai")


@plugins.hookimpl(specname="plug_cli")
@only_host("dlthub")
def plug_cli_ai(host: str) -> Optional[Type[plugins.SupportsCliCommand]]:
    from dlt._workspace.cli.dlthub.commands import AiCommand

    return AiCommand


@plugins.hookimpl(specname="plug_cli")
@only_host("dlthub")
def plug_cli_dlthub_init(host: str) -> Optional[Type[plugins.SupportsCliCommand]]:
    # always available — used to bootstrap a workspace
    from dlt._workspace.cli.dlthub.commands import InitWorkspaceCommand

    return InitWorkspaceCommand


@plugins.hookimpl(specname="plug_cli")
@only_host("dlthub")
def plug_cli_dlthub_pipeline(host: str) -> Optional[Type[plugins.SupportsCliCommand]]:
    if not is_workspace_active():
        return None
    from dlt._workspace.cli.dlthub.commands import PipelineCommand

    return PipelineCommand


@plugins.hookimpl(specname="plug_cli")
@only_host("dlthub")
def plug_cli_dlthub_local(host: str) -> Optional[Type[plugins.SupportsCliCommand]]:
    if not is_workspace_active():
        return None
    from dlt._workspace.cli.dlthub.commands import LocalWorkspaceCommand

    return LocalWorkspaceCommand


@plugins.hookimpl(specname="plug_cli")
@only_host("dlthub")
def plug_cli_dlthub_profile(host: str) -> Optional[Type[plugins.SupportsCliCommand]]:
    if not is_workspace_active():
        return None
    from dlt._workspace.cli.dlthub.commands import ProfileCommand

    return ProfileCommand
