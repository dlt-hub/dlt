import os
from typing import ClassVar, Optional, Sequence, Tuple

from dlt.common.configuration.container import Container
from dlt.common.configuration.providers import CONFIG_TOML, SECRETS_TOML, EnvironProvider
from dlt.common.configuration.providers.toml import GLOBAL_ORIGIN_PREFIX, SettingsTomlProvider
from dlt.common.configuration.resolve import _resolve_configuration
from dlt.common.configuration.specs import known_sections
from dlt.common.configuration.specs.base_configuration import (
    BaseConfiguration,
    NotResolved,
    configspec,
)
from dlt.common.configuration.specs.pluggable_run_context import PluggableRunContext
from dlt.common.configuration.specs.runtime_configuration import RuntimeConfiguration
from dlt.common.typing import Annotated, Literal, TSecretStrValue

from dlt._workspace.exceptions import WorkspaceIdConflict
from dlt._workspace.typing import DEFAULT_DEPLOYMENT_MODULE

ENVIRONMENT_SECTION = "environment"
"""The settings of each environment are under `[workspace.environment.<name>]`."""


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
    """Settings of an environment, `[workspace.environment.<name>]`: the remote workspace that
    the local workspace is connected to and the deployment module."""

    workspace_id: Optional[str] = None
    name: Optional[str] = None
    """Name of the remote workspace, as `name` under `[workspace.settings]` names the local one."""
    deployment_module: str = DEFAULT_DEPLOYMENT_MODULE
    """The deployment module that dlt imports to generate the deployment manifest of the
    environment: a module name or a `.py` path relative to the workspace."""
    deploy_dashboard: Optional[bool] = None
    """Deploys the workspace dashboard with the deployment module of the environment. Defaults to
    true for the default environment when it is not strict."""


def _get_environment_workspace_name(name: str, environment: str) -> str:
    return f"{name}-{environment}"


def resolve_environment_configuration(environment: str) -> WorkspaceEnvironmentConfiguration:
    """Resolves the config of `environment` with the active providers: its remote
    workspace connection and its deployment module."""
    # embedded-config lookup: falls back to `[workspace.<env>]` and `[<env>]`, never to the root
    return _resolve_configuration(
        WorkspaceEnvironmentConfiguration(),
        (known_sections.WORKSPACE, ENVIRONMENT_SECTION),
        (environment,),
        None,
        False,
    )


TValueSource = Literal["environ", "profile", "shared"]


def _get_value_origin(key: str, section: str) -> Optional[Tuple[TValueSource, str]]:
    """Where the active providers resolve `key` of the workspace `section` from, and the
    location of the value. The source is `environ`, `profile` for a settings file of the active
    profile, or `shared` for a source that all profiles share. The location is a settings file
    or the name of another provider. `None` when no provider has the value."""
    providers = Container()[PluggableRunContext].providers.providers
    for provider in providers:
        for sections in ((known_sections.WORKSPACE, section), (section,)):
            value, _ = provider.get_value(key, str, None, *sections)
            if value is None:
                continue
            if isinstance(provider, EnvironProvider):
                return "environ", provider.get_value_location(key, None, *sections)
            if not isinstance(provider, SettingsTomlProvider):
                # a vault or a plugin provider serves every profile
                return "shared", provider.name
            location = provider.get_value_location(key, None, *sections)
            file_name = os.path.basename(location.removeprefix(GLOBAL_ORIGIN_PREFIX))
            return ("shared" if file_name in (CONFIG_TOML, SECRETS_TOML) else "profile"), location
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
    deployment_module: Annotated[Optional[str], NotResolved()] = None
    """Deployment module of the environment of the active profile, `None` for a local profile"""
    deploy_dashboard: Annotated[Optional[bool], NotResolved()] = None
    """Whether the environment of the active profile deploys the workspace dashboard, `None` for a
    local profile"""

    __recommended_sections__: ClassVar[Sequence[str]] = (known_sections.WORKSPACE,)

    def on_resolved(self) -> None:
        if not self.environment:
            return
        connection = resolve_environment_configuration(self.environment)
        self.deployment_module = connection.deployment_module
        self.deploy_dashboard = connection.deploy_dashboard
        is_default = self.environment == self.default_environment
        id_origin = _get_value_origin("workspace_id", WorkspaceRuntimeConfiguration.__section__)
        if id_origin and id_origin[0] == "environ":
            # the platform and CI name the remote workspace of the process with a variable
            if connection.workspace_id not in (None, self.runtime.workspace_id):
                # the section describes another remote workspace: neither its id nor its name
                # apply
                connection = WorkspaceEnvironmentConfiguration()
        elif is_default and id_origin and connection.workspace_id:
            # `[runtime]` in a file, set before environments existed, connects the local workspace
            # in the default environment. dlt refuses a second id there, even an equal one
            raise WorkspaceIdConflict(self.environment, id_origin[1])
        elif connection.workspace_id:
            self.runtime.workspace_id = connection.workspace_id
        elif not is_default and id_origin and id_origin[0] == "shared":
            # a shared `[runtime]` belongs to the default environment. One in the file of the
            # profile connects the local workspace in the environment of the profile
            self.runtime.workspace_id = None
        # a name that the profile resolves from its own file or from the environment variables is
        # its own. one from a shared file, or the folder name, belongs to the default environment
        name = self.settings.name or self.name
        if connection.name:
            self.settings.name = connection.name
        elif name:
            name_origin = _get_value_origin("name", "settings")
            own_name = name_origin is not None and name_origin[0] in ("environ", "profile")
            self.settings.name = (
                name
                if is_default or own_name
                else _get_environment_workspace_name(name, self.environment)
            )
