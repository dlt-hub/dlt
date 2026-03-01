from typing import Dict

from pydantic import Field
from fastmcp.exceptions import ToolError

from dlt.common.typing import Annotated
from dlt._workspace.cli.ai.utils import (
    DEFAULT_AI_WORKBENCH_BRANCH,
    DEFAULT_AI_WORKBENCH_REPO,
    fetch_workbench_base,
    fetch_workbench_toolkit_info,
    scan_workbench_toolkits,
)
from dlt._workspace.mcp.tools._context import with_mcp_tool_telemetry
from dlt._workspace.typing import TToolkitInfo, TWorkbenchToolkitInfo


@with_mcp_tool_telemetry()
def list_toolkits() -> Dict[str, TToolkitInfo]:
    """List available dlt AI toolkits with their names and descriptions.

    Fetches the toolkit index from the dlt AI workbench repository."""
    try:
        base = fetch_workbench_base(DEFAULT_AI_WORKBENCH_REPO, DEFAULT_AI_WORKBENCH_BRANCH)
    except FileNotFoundError as ex:
        raise ToolError(str(ex))
    return scan_workbench_toolkits(base, listed_only=True)


@with_mcp_tool_telemetry()
def toolkit_info(
    name: Annotated[str, Field(description="Name of the toolkit to inspect")],
) -> TWorkbenchToolkitInfo:
    """Show detailed contents of a dlt AI toolkit including skills, commands,
    rules, and MCP server definitions."""
    try:
        info = fetch_workbench_toolkit_info(
            name, DEFAULT_AI_WORKBENCH_REPO, DEFAULT_AI_WORKBENCH_BRANCH
        )
    except (FileNotFoundError, ValueError) as ex:
        raise ToolError(str(ex))
    if info is None:
        raise ToolError("Toolkit '%s' not found" % name)
    return info


__tools__ = (list_toolkits, toolkit_info)
