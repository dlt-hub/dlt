from fastmcp import FastMCP

server = FastMCP(
    "data-tools",
    instructions="Tools for querying the data warehouse",
    version="1.0.0",
)


@server.tool
def query(sql: str) -> str:
    return f"executed: {sql}"
