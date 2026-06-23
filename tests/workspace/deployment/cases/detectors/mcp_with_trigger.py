"""MCP server with additional triggers."""

from fastmcp import FastMCP

mcp = FastMCP("test-tools")

__trigger__ = ["tag:tools", "deployment"]
