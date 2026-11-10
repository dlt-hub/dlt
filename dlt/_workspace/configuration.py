import os
from typing import ClassVar, Optional, Sequence

from dlt.common.configuration.container import Container
from dlt.common.configuration.providers import CONFIG_TOML, SECRETS_TOML, EnvironProvider
from dlt.common.configuration.providers.toml import GLOBAL_ORIGIN_PREFIX
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.configuration.specs import known_sections
from dlt.common.configuration.specs.base_configuration import (
    BaseConfiguration,
    NotResolved,
    configspec,
)
from dlt.common.configuration.specs.pluggable_run_context import PluggableRunContext
from dlt.common.configuration.specs.runtime_configuration import RuntimeConfiguration
from dlt.common.typing import Annotated, Literal, TSecretStrValue

ENVIRONMENT_SECTION = "environment"
"""Per environment, the local workspace is connected to a remote workspace under
`[workspace.environment.<name>]`."""


@configspec
class WorkspaceSettings(BaseConfiguration):
    name: Optional[str] = None
    """Workspace name. After resolution, the name of the remote workspace in the environment of
    the active profile."""
    # TODO: implement default profile switch. it requires reading the configuration, discovering
    # the profile and then recreating the workspace context. since this functionality is not
    # immediately needed it will be skipped for now
    # default_profile: Optional[str] = None
    working_dir: Optional[str] = None
    """Pipeline state and other writable runtime files. Defaults to `.dlt/state/<profile>`. Relative to workspace root."""
    local_dir: Optional[str] = None
    """Local destination data (e.g. duckdb files). Defaults to `.dlt/data/<profile>`. Relative to workspace root."""


@configspec
class WorkspaceRuntimeConfiguration(RuntimeConfiguration):
    """Extends runtime configuration with dlthub runtime"""

    workspace_id: Optional[str] = None
    """Id of the remote workspace in the environment of the active profile, `None` when the local
    workspace is not connected in it"""
    organization_id: Optional[str] = None
    """Id of the organization of the remote workspace"""
    auth_token: Optional[TSecretStrValue] = None
    """JWT token for Runtime API"""
    api_key: Optional[TSecretStrValue] = None
    """API key for Runtime API"""
    api_base_url: Optional[str] = "https://api.dlthub.com"
    """Base URL for the dltHub Runtime API"""
    ai_context_api_url: Optional[str] = "https://scaffold.apps.dlthub.com"
    """Base URL for the AI context documentation API"""
    invite_code: Optional[str] = None
    """Invite code for dltHub Runtime"""

    __section__: ClassVar[str] = "runtime"


@configspec
class WorkspaceEnvironmentConfiguration(BaseConfiguration):
    """The remote workspace that the local workspace is connected to in an environment,
    `[workspace.environment.<name>]`."""

    workspace_id: Optional[str] = None
    workspace_name: Optional[str] = None


def _environment_workspace_name(name: str, environment: str) -> str:
    return f"{name}-{environment}"


def resolve_environment_connection(environment: str) -> WorkspaceEnvironmentConfiguration:
    """Resolves the remote workspace connection of `environment` with the active providers."""
    return resolve_configuration(
        WorkspaceEnvironmentConfiguration(),
        sections=(known_sections.WORKSPACE, ENVIRONMENT_SECTION, environment),
    )


TValueSource = Literal["environ", "profile", "shared"]


def _value_source(key: str, section: str) -> Optional[TValueSource]:
    """Where the active providers resolve `key` of the workspace `section` from: the environment
    variables, a settings file of the active profile, or a file that all profiles share. `None`
    when no provider has the value."""
    providers = Container()[PluggableRunContext].providers.providers
    for provider in providers:
        for sections in ((known_sections.WORKSPACE, section), (section,)):
            value, _ = provider.get_value(key, str, None, *sections)
            if value is None:
                continue
            if isinstance(provider, EnvironProvider):
                return "environ"
            location = provider.get_value_location(key, None, *sections)
            file_name = os.path.basename(location.removeprefix(GLOBAL_ORIGIN_PREFIX))
            return "shared" if file_name in (CONFIG_TOML, SECRETS_TOML) else "profile"
    return None


@configspec
class WorkspaceConfiguration(BaseConfiguration):
    settings: WorkspaceSettings = None
    runtime: WorkspaceRuntimeConfiguration = None
    # NOTE: is resolved separately but in the same layout
    # dashboard: DashboardConfiguration
    # TODO: launch workspace mcp using mcp configuration
    # mcp_config: McpConfiguration
    name: Annotated[Optional[str], NotResolved()] = None
    """Workspace name derived from the folder, the run context injects it"""
    environment: Annotated[Optional[str], NotResolved()] = None
    """Environment of the active profile, `None` for a local profile"""
    default_environment: Annotated[Optional[str], NotResolved()] = None

    __recommended_sections__: ClassVar[Sequence[str]] = (known_sections.WORKSPACE,)

    def on_resolved(self) -> None:
        if not self.environment:
            return
        connection = resolve_environment_connection(self.environment)
        is_default = self.environment == self.default_environment
        # the platform and CI name the remote workspace of the process with a variable
        id_source = _value_source("workspace_id", WorkspaceRuntimeConfiguration.__section__)
        process_id = id_source == "environ"
        if process_id and connection.workspace_id not in (None, self.runtime.workspace_id):
            # the section describes another remote workspace: neither its id nor its name apply
            connection = WorkspaceEnvironmentConfiguration()
        # a name or an id that the profile resolves from its own file or from the environment
        # variables is its own. one from a shared file belongs to the default environment
        name = self.settings.name or self.name
        if connection.workspace_name:
            self.settings.name = connection.workspace_name
        elif name:
            own_name = _value_source("name", "settings") in ("environ", "profile")
            self.settings.name = (
                name
                if is_default or own_name
                else _environment_workspace_name(name, self.environment)
            )
        if process_id:
            return
        if connection.workspace_id:
            self.runtime.workspace_id = connection.workspace_id
        elif id_source == "shared" and not is_default:
            self.runtime.workspace_id = None
