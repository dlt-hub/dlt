from typing import Any, Dict, List

from pydantic import Field
from fastmcp.exceptions import ToolError

from dlt.common.typing import Annotated
from dlt._workspace.cli.ai.utils import (
    DEFAULT_AI_WORKBENCH_BRANCH,
    DEFAULT_AI_WORKBENCH_REPO,
    fetch_workbench_toolkit_info,
    fetch_workbench_toolkit_list,
)
from dlt._workspace.mcp.tools._context import with_mcp_tool_telemetry
from dlt._workspace.typing import TWorkbenchToolkitInfo


@with_mcp_tool_telemetry()
def list_toolkits() -> List[Dict[str, Any]]:
    """List available dlt AI toolkits with their names and descriptions.

    Fetches the toolkit index from the dlt AI workbench repository."""
    result = fetch_workbench_toolkit_list(DEFAULT_AI_WORKBENCH_REPO, DEFAULT_AI_WORKBENCH_BRANCH)
    if result is None:
        raise ToolError("Could not fetch AI workbench repository")
    return result


@with_mcp_tool_telemetry()
def toolkit_info(
    name: Annotated[str, Field(description="Name of the toolkit to inspect")],
) -> TWorkbenchToolkitInfo:
    """Show detailed contents of a dlt AI toolkit including skills, commands,
    rules, and MCP server definitions."""
    info = fetch_workbench_toolkit_info(
        name, DEFAULT_AI_WORKBENCH_REPO, DEFAULT_AI_WORKBENCH_BRANCH
    )
    if info is None:
        raise ToolError("Toolkit '%s' not found" % name)
    return info


__tools__ = (list_toolkits, toolkit_info)
