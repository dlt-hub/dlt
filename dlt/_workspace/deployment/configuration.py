"""Framework-specific configuration specs for job launchers.

Each spec resolves in sections (JOBS, section, name, __section__) so users
can override per-job via env vars like JOBS__MY_MODULE__MY_JOB__MCP__HOST.
"""

import dataclasses
from typing import Any, Dict, Literal, Optional

from dlt.common.configuration import configspec
from dlt.common.configuration.specs.base_configuration import BaseConfiguration
from dlt.common.pipeline import TRefreshMode
from dlt.common.typing import TSecretStrValue

from dlt._workspace.deployment.typing import TIncrementalSource


@configspec
class JobConfiguration(BaseConfiguration):
    """Job behavior settings resolvable from config in `jobs` sections."""

    incremental_mode: Optional[TIncrementalSource] = None
    """How incrementals obtain their range during a job run."""
    auto_refresh_pipeline_mode: Optional[TRefreshMode] = None
    """Refresh mode applied to every pipeline in the job when a refresh run is requested."""


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
    asset_url: Optional[str] = "https://cdn.jsdelivr.net/npm/@marimo-team/frontend@{version}/dist"
    """CDN URL for marimo frontend assets. Marimo substitutes `{version}` from the installed package. Set to empty string to disable and serve assets from the app origin."""


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


USER_ENDPOINT_FIELDS = ("model", "api_key", "api_url", "api_version")
"""The model and the credentials that reach it. A run takes all four from the user or all four
from the runtime, and never one from each: a runtime model id means nothing to another endpoint."""


@configspec
class AgentConfiguration(BaseConfiguration):
    """Configuration for the background agent launcher."""

    __section__ = "agent"

    loop: Optional[str] = None
    """Loop implementation, e.g. "pydantic-ai" or "claude-agent-sdk"."""
    instructions: Optional[str] = None
    """What to tell the agent to do: the user turn, sent as the run's first message."""
    model: Optional[str] = None
    """`provider:model` id, or an alias (sonnet, opus, haiku, fable, gpt, gemini)."""
    api_key: Optional[TSecretStrValue] = None
    """Key for the provider the model names. Its own env var is used when unset."""
    api_url: Optional[str] = None
    """Base URL of the model API, for a proxy or a private deployment."""
    api_version: Optional[str] = None
    """API version the provider requires. Azure needs one; no other provider takes it."""
    runtime_model: Optional[str] = None
    """Model supplied by the runtime. Both live side by side; see `USER_ENDPOINT_FIELDS`."""
    runtime_api_key: Optional[TSecretStrValue] = None
    """Key supplied by the runtime. Both live side by side; see `USER_ENDPOINT_FIELDS`."""
    runtime_api_url: Optional[str] = None
    """Base URL supplied by the runtime. Both live side by side; see `USER_ENDPOINT_FIELDS`."""
    runtime_api_version: Optional[str] = None
    """Version supplied by the runtime. Both live side by side; see `USER_ENDPOINT_FIELDS`."""
    max_turns: Optional[int] = None
    max_tokens: Optional[int] = None
    loop_run_args: Dict[str, Any] = dataclasses.field(default_factory=dict)
    """Arguments passed to the native loop, merged over the agent's own defaults."""
    verbosity: Optional[int] = None
    """How much of the run to show: 0 quiet, 1 thoughts and tool detail, 2 everything."""
    trace_url: Optional[str] = None
    """OTLP endpoint the loop exports its spans to."""
    trace_key: Optional[TSecretStrValue] = None

    @property
    def endpoint_source(self) -> str:
        """Whose endpoint a run uses: `"user"` when it set any field of one, else `"runtime"`."""
        return "user" if any(getattr(self, f) for f in USER_ENDPOINT_FIELDS) else "runtime"

    @property
    def effective_model(self) -> Optional[str]:
        # None here means the agent's own model applies, never the runtime's
        return self.model if self.endpoint_source == "user" else self.runtime_model

    @property
    def effective_api_key(self) -> Optional[str]:
        return self.api_key if self.endpoint_source == "user" else self.runtime_api_key

    @property
    def effective_api_url(self) -> Optional[str]:
        return self.api_url if self.endpoint_source == "user" else self.runtime_api_url

    @property
    def effective_api_version(self) -> Optional[str]:
        return self.api_version if self.endpoint_source == "user" else self.runtime_api_version
