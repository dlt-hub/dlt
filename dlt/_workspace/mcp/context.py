import contextlib
import functools
import threading
import time
from typing import Any, Callable, Dict, Optional

from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs import RuntimeConfiguration
from dlt.common.configuration.specs.pluggable_run_context import PluggableRunContext
from dlt.common.runtime.anon_tracker import track
from dlt.common.runtime.telemetry import _TELEMETRY_STARTED, start_telemetry
from dlt.common.typing import TFun
from dlt.common.utils import digest128

# serialize context reloads for concurrent MCP tool calls
_context_reload_lock = threading.Lock()


def _get_agent_info() -> Optional[Dict[str, str]]:
    """Extract connecting client metadata from the MCP session.

    Reads ``clientInfo`` sent by the MCP client during the ``initialize``
    handshake.  The session id is hashed so the raw value is never stored.

    Returns:
        A dict with ``name``, ``version``, and ``session_hash`` when a
        session is active, or ``None`` outside of an MCP request.
    """
    try:
        from fastmcp.server.context import _current_context

        ctx = _current_context.get(None)
        if ctx is None:
            return None
        session = ctx.session
        client_params = session.client_params
        if client_params is None:
            return None
        info: Dict[str, str] = {
            "name": client_params.clientInfo.name,
            "version": client_params.clientInfo.version,
        }
        session_id = ctx.session_id
        if session_id:
            info["session_hash"] = digest128(session_id)
        return info
    except Exception:
        return None


def with_mcp_tool_telemetry(**extra_props: Any) -> Callable[[TFun], TFun]:
    """Telemetry decorator for MCP tool functions.

    Emits an anonymous ``mcp_tool_{name}`` telemetry event after each tool
    invocation.  Before executing the tool the decorator fully reloads the
    runtime context from disk (workspace marker, profile pin, secrets /
    config files) so that every MCP call sees the latest on-disk state.
    Reload and execution are serialized under a module-level lock.

    Event shape (``category="mcp"``, ``event_name="tool_{fn.__name__}"``)::

        {
            "event": "mcp_tool_secrets_list",
            "properties": {
                "event_category": "mcp",
                "event_name": "tool_secrets_list",
                "mcp_primitive": "tool",
                "elapsed": 0.023,
                "success": True,
                "agent_info": {
                    "name": "claude-ai",
                    "version": "1.0.0",
                    "session_hash": "abc123...",
                },
            },
        }

    ``agent_info`` is ``None`` when the session metadata is not available
    (e.g. during tests that call tool functions directly).

    Args:
        **extra_props: literal key-value pairs added to the telemetry event
            properties.

    Returns:
        A decorator that wraps the tool function with telemetry tracking and
        context reload.
    """

    def decorator(fn: TFun) -> TFun:
        event_name = "tool_" + fn.__name__

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            props: Dict[str, Any] = {"mcp_primitive": "tool"}
            props.update(extra_props)
            props["agent_info"] = _get_agent_info()
            start_ts = time.time()

            def _track(success: bool) -> None:
                with contextlib.suppress(Exception):
                    props["elapsed"] = time.time() - start_ts
                    props["success"] = success
                    if not _TELEMETRY_STARTED:
                        c = resolve_configuration(RuntimeConfiguration())
                        start_telemetry(c)
                    track("mcp", event_name, props)

            with _context_reload_lock:
                ctx_plug = Container()[PluggableRunContext]
                ctx_plug.reload(ctx_plug.context.run_dir)

            try:
                rv = fn(*args, **kwargs)
                _track(True)
                return rv
            except Exception:
                _track(False)
                raise

        return wrapper  # type: ignore[return-value]

    return decorator
