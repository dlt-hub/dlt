from typing import ClassVar, Optional, Sequence
from dlt.common.configuration.specs import known_sections
from dlt.common.configuration.specs.base_configuration import BaseConfiguration, configspec
from dlt.common.configuration.specs.runtime_configuration import RuntimeConfiguration
from dlt.common.typing import TSecretStrValue


@configspec
class WorkspaceSettings(BaseConfiguration):
    name: Optional[str] = None
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
    """Id of the remote workspace that local one should be connected to"""
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
class WorkspaceConfiguration(BaseConfiguration):
    settings: WorkspaceSettings = None
    runtime: WorkspaceRuntimeConfiguration = None
    # NOTE: is resolved separately but in the same layout
    # dashboard: DashboardConfiguration
    # TODO: launch workspace mcp using mcp configuration
    # mcp_config: McpConfiguration

    __recommended_sections__: ClassVar[Sequence[str]] = (known_sections.WORKSPACE,)
