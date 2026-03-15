from fastmcp import FastMCP

mcp = FastMCP("simple-mcp")


@mcp.tool
def ping() -> str:
    return "pong"
