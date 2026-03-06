from typing import Any, Dict, List, Literal, NamedTuple, Optional
from typing_extensions import NotRequired

from dlt.common.configuration.providers.provider import ConfigProvider
from dlt.common.storages.configuration import TSchemaFileFormat
from dlt.common.typing import TypedDict


TLocationScope = Literal["project", "global"]

TAiStatusWarning = Literal[
    "not_initialized",
    "no_init_toolkit",
    "no_toolkits",
    "mcp_unavailable",
]


class TAiStatusInfo(TypedDict):
    """AI setup status: version, agent, toolkits, and readiness warnings."""

    dlt_version: str
    agent_name: Optional[str]
    initialized: bool
    has_init_toolkit: bool
    toolkits: Dict[str, "TToolkitIndexEntry"]
    warnings: List[TAiStatusWarning]
    mcp_error: NotRequired[str]


class TLocationInfo(TypedDict):
    """A single config file location with its scope and presence status."""

    path: str
    present: bool
    scope: TLocationScope
    profile_name: NotRequired[str]


class TProviderInfo(TypedDict):
    """Config provider with all its file locations."""

    name: str
    is_empty: bool
    locations: List[TLocationInfo]


class TProfileInfo(TypedDict):
    """A single profile with its status flags."""

    name: str
    description: str
    is_current: bool
    is_pinned: bool
    is_configured: bool


class TCurrentProfileInfo(TProfileInfo):
    """The active profile, extending base with session directories."""

    data_dir: str
    local_dir: str


class TWorkbenchComponentInfo(TypedDict):
    """A skill, command, or rule inside a workbench toolkit."""

    name: str
    description: str


class TWorkbenchMcpServerInfo(TypedDict):
    """MCP server definition inside a workbench toolkit."""

    command: str
    args: List[str]


class TToolkitInfo(TypedDict):
    """Core toolkit info from plugin.json + toolkit.json."""

    name: str
    version: str
    description: str
    tags: List[str]
    dependencies: NotRequired[List[str]]
    workflow_entry_skill: NotRequired[str]


class TWorkbenchToolkitInfo(TToolkitInfo):
    """Extends meta with structural data from directory scan."""

    skills: List[TWorkbenchComponentInfo]
    commands: List[TWorkbenchComponentInfo]
    rules: List[TWorkbenchComponentInfo]
    mcp_servers: NotRequired[Dict[str, TWorkbenchMcpServerInfo]]
    has_ignore: bool


class TToolkitIndexEntry(TToolkitInfo, total=False):
    """Installed toolkit record. Inherits required meta fields, adds optional tracking."""

    installed_at: str
    agent: str
    files: Dict[str, Any]
    mcp_servers: List[str]


class TWorkspaceInfo(TypedDict):
    """Full workspace state returned by `fetch_workspace_info`."""

    name: Optional[str]
    run_dir: str
    settings_dir: str
    global_dir: str
    profile: Optional[TCurrentProfileInfo]
    configured_profiles: List[str]
    providers: List[TProviderInfo]
    dlt_version: str
    dlthub_version: Optional[str]
    initialized: bool
    installed_toolkits: Dict[str, TToolkitIndexEntry]


class TSourceItem(TypedDict):
    """A source returned by the AI context search API."""

    source_name: str
    description: Optional[str]
    description_verbose: NotRequired[str]
    sample_urls: NotRequired[str]


class TSchemaExport(TypedDict):
    """Exported schema in a requested format."""

    schema_name: str
    format_: TSchemaFileFormat
    content: str


class ProviderLocationInfo(NamedTuple):
    path: str
    present: bool
    scope: TLocationScope
    profile_name: Optional[str]


class ProviderInfo(NamedTuple):
    provider: ConfigProvider
    locations: List[ProviderLocationInfo]
