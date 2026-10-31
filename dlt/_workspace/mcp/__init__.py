from dlt.common.exceptions import MissingDependencyException

try:
    from fastmcp import FastMCP
    from dlt._workspace.mcp.server import DltMCP, WorkspaceMCP, PipelineMCP
except ModuleNotFoundError:
    raise MissingDependencyException(
        "dlthub mcp support",
        ["fastmcp"],
    )

__all__ = ["FastMCP", "DltMCP", "WorkspaceMCP", "PipelineMCP"]
