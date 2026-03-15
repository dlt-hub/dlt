"""Interactive jobs for the test workspace."""

from dlt._workspace.deployment.decorators import interactive


@interactive(interface="rest_api")
def api_server():
    """REST API server."""
    pass


@interactive(interface="mcp")
def mcp_tools():
    """MCP tool server."""
    pass
