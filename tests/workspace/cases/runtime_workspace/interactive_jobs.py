"""Interactive jobs for the test workspace."""

from dlt.hub.run import interactive


@interactive(interface="rest_api")
def api_server():
    """REST API server."""
    pass


@interactive(interface="mcp")
def mcp_tools():
    """MCP tool server."""
    pass
