"""Framework-specific configuration specs for interactive job launchers.

Each spec resolves in sections (JOBS, section, name, __section__) so users
can override per-job via env vars like JOBS__MY_MODULE__MY_JOB__MCP__HOST.
"""

from typing import Literal, Optional

from dlt.common.configuration import configspec
from dlt.common.configuration.specs.base_configuration import BaseConfiguration


@configspec
class MarimoConfiguration(BaseConfiguration):
    """Configuration for the marimo launcher."""

    __section__ = "marimo"

    include_code: bool = False
    """Show notebook source code in the app."""
    token: Optional[str] = None
    """Auth token for session access. None disables auth."""
    session_ttl: int = 120
    """Seconds before closing an idle session."""
    command: Literal["run", "edit"] = "run"


@configspec
class StreamlitConfiguration(BaseConfiguration):
    """Configuration for the streamlit launcher."""

    __section__ = "streamlit"

    enable_cors: bool = False
    """Enable CORS. Disabled by default behind a proxy."""
    enable_xsrf_protection: bool = False
    """Enable XSRF protection. Disabled by default behind a proxy."""
    gather_usage_stats: bool = False
    """Send usage statistics to Streamlit."""


@configspec
class McpConfiguration(BaseConfiguration):
    """Configuration for the FastMCP launcher."""

    __section__ = "mcp"

    transport: str = "http"
    """Transport protocol: "stdio", "http", "sse", or "streamable-http"."""
    path: str = "/mcp"
    """HTTP endpoint route path."""
    log_level: str = "INFO"
    """Log level: DEBUG, INFO, WARNING, ERROR, CRITICAL."""
    stateless_http: bool = False
    """Stateless mode for horizontal scaling (no session affinity)."""
