from fastmcp import FastMCP as MCPServer

my_tools = MCPServer("aliased-server")


@my_tools.tool
def echo(text: str) -> str:
    return text
