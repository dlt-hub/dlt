"""MCP server using app variable name."""

from fastmcp import FastMCP

app = FastMCP("app-named-server")


@app.tool
def status() -> str:
    return "ok"
