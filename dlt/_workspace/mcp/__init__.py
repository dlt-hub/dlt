from dlt import version
from dlt.common.exceptions import MissingDependencyException

try:
    from mcp.server.fastmcp import FastMCP
    from dlt._workspace.mcp.server import DltMCP, WorkspaceMCP, PipelineMCP
except ModuleNotFoundError:
    raise MissingDependencyException(
        "dlt mcp support",
        [f"{version.DLT_PKG_NAME}[workspace]"],
        "Install dlt with Workspace extras to use MCP",
    )

__all__ = ["FastMCP", "DltMCP", "WorkspaceMCP", "PipelineMCP"]
