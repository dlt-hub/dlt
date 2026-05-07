from fastmcp import FastMCP

mcp = FastMCP("test-tools", instructions="Tools for testing")


@mcp.tool
def ping() -> str:
    return "pong"
