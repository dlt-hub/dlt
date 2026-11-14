from typing import Any, Dict, List, Literal, Optional

from dlt.common.typing import NotRequired, TypedDict


TAIAgentName = Literal["claude", "cursor", "codex", "cortex"]
"""Names of the AI coding agents a toolkit can be installed for."""


class TAgentVisibility(TypedDict, total=False):
    """Agents that see a toolkit or a component: only `include`, or all but `exclude`."""

    include: List[str]
    exclude: List[str]


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
    """A skill, command, rule, or agent inside a dltHub AI Harness toolkit."""

    name: str
    description: str


class TWorkbenchMcpServerInfo(TypedDict):
    """MCP server definition inside a dltHub AI Harness toolkit."""

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
    agent_visibility: NotRequired[TAgentVisibility]
    """Agents that see the toolkit, from `agents` in `toolkit.json`."""
    component_visibility: NotRequired[Dict[str, TAgentVisibility]]
    """Agents that see single components, keyed by source folder and name (`skills/setup`)."""
    mcp_features: NotRequired[Dict[str, List[str]]]
    """Features the toolkit adds to MCP servers of other toolkits, keyed by server name, from
    `mcp_features` in `toolkit.json`."""


class TWorkbenchToolkitInfo(TToolkitInfo):
    """Extends meta with structural data from directory scan."""

    skills: List[TWorkbenchComponentInfo]
    commands: List[TWorkbenchComponentInfo]
    rules: List[TWorkbenchComponentInfo]
    agents: List[TWorkbenchComponentInfo]
    mcp_servers: NotRequired[Dict[str, TWorkbenchMcpServerInfo]]
    has_ignore: bool


class TToolkitIndexEntry(TToolkitInfo, total=False):
    """Installed toolkit record. Inherits required meta fields, adds optional tracking."""

    installed_at: str
    agent: str
    files: Dict[str, Any]
    mcp_servers: List[str]
    see_all: bool
    """Installed with `--see-all`: updates keep components hidden from the agent."""
