from typing import Any, Dict, List, Literal, Optional

from dlt.common.typing import NotRequired, TypedDict


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
