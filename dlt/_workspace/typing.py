from typing import Any, Dict, List, Literal, Optional
from typing_extensions import NotRequired

from dlt.common.typing import TypedDict


TLocationScope = Literal["project", "global"]


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
    """Active workspace profile with its directories and settings."""

    name: str
    is_pinned: bool
    data_dir: str
    local_dir: str
    configured_profiles: List[str]


class TWorkbenchComponentInfo(TypedDict):
    """A skill, command, or rule inside a workbench toolkit."""

    name: str
    description: str


class TWorkbenchMcpServerInfo(TypedDict):
    """MCP server definition inside a workbench toolkit."""

    command: str
    args: List[str]


class TToolkitInfo(TypedDict):
    """Core toolkit identity from plugin.json + toolkit.json."""

    name: str
    version: str
    description: str
    tags: List[str]
    workflow_entry_skill: NotRequired[str]


class TWorkbenchToolkitInfo(TToolkitInfo):
    """Extends meta with structural data from directory scan."""

    dependencies: List[str]
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
    profile: Optional[TProfileInfo]
    providers: List[TProviderInfo]
    dlt_version: str
    dlthub_version: Optional[str]
    initialized: bool
    installed_toolkits: Dict[str, TToolkitIndexEntry]
