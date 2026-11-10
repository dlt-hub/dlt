import contextlib
import functools
import inspect
import threading
import time
from typing import Any, Callable, Dict, Optional

from pydantic import Field

from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs import RuntimeConfiguration
from dlt.common.configuration.specs.pluggable_run_context import PluggableRunContext
from dlt.common.runtime.anon_tracker import track
from dlt.common.runtime.telemetry import _TELEMETRY_STARTED, start_telemetry
from dlt.common.typing import Annotated, TFun
from dlt.common.utils import custom_environ, digest128

from dlt._workspace._known_env import WORKSPACE__PROFILE
from dlt._workspace._workspace_context import WorkspaceRunContext
from dlt._workspace.exceptions import ProfileException
from dlt._workspace.environment import ENVIRONMENT_ARGUMENT_HELP, select_environment
from dlt._workspace.profile import PROFILE_ARGUMENT_HELP, check_profile_declared
from dlt._workspace.workspace_definition import PROFILE_NAME_RE, default_profile

# serialize context reloads and tool bodies: the run context is global
_context_reload_lock = threading.Lock()

TProfileArgument = Annotated[Optional[str], Field(description=PROFILE_ARGUMENT_HELP)]
"""The `profile` argument of a tool that works with a profile. `with_mcp_tool_telemetry`
overrides the active profile with it for the call. Outside of a workspace the server hides it
from the tool schema."""

TEnvironmentArgument = Annotated[Optional[str], Field(description=ENVIRONMENT_ARGUMENT_HELP)]
"""The `environment` argument of a tool that deploys or works with a deployment.
`with_mcp_tool_telemetry` activates the default write profile of that environment for the call.
When the call names none, dlt takes the environment of the active profile, else the default one."""


def _get_agent_info() -> Optional[Dict[str, str]]:
    """Extract connecting client metadata from the MCP session.

    Reads `clientInfo` sent by the MCP client during the `initialize`
    handshake.  The session id is hashed so the raw value is never stored.

    Returns:
        A dict with `name`, `version`, and `session_hash` when a
        session is active, or `None` outside of an MCP request.
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


def _activate_call_profile(
    ctx_plug: PluggableRunContext, name: Optional[str], environment: bool
) -> str:
    """Reloads the run context with the profile that `name` selects: the profile itself, or the
    default write profile of an environment. Raises `ToolError` with the remedy."""
    from fastmcp.exceptions import ToolError

    argument = "environment" if environment else "profile"
    ctx = ctx_plug.context
    if not isinstance(ctx, WorkspaceRunContext):
        raise ToolError(
            f"The `{argument}` argument needs a dltHub workspace and this MCP server runs outside"
            " of one. Run `dlthub init` in the project folder to create a workspace."
        )
    if name is not None and not PROFILE_NAME_RE.match(name):
        raise ToolError(
            f"`{name}` is not {'an' if environment else 'a'} {argument} name: use letters, digits,"
            " `_` and `-`. The `list_profiles` tool shows the declared profiles with their"
            " environment."
        )
    try:
        if environment:
            profile = default_profile(select_environment(name), "write")
        else:
            check_profile_declared(name)
            profile = name
    except ProfileException as ex:
        raise ToolError(str(ex)) from ex
    if profile != ctx.profile:
        ctx_plug.reload(ctx.run_dir, runtime_kwargs={"profile": profile})
    return profile


def with_mcp_tool_telemetry(**extra_props: Any) -> Callable[[TFun], TFun]:
    """Telemetry decorator for MCP tool functions.

    Emits an anonymous `mcp_tool_{name}` telemetry event after each tool
    invocation. Before the tool runs, the decorator reloads the run context
    from disk, so every MCP call sees the latest on-disk state. A tool with a
    `profile` argument (`TProfileArgument`) or an `environment` argument
    (`TEnvironmentArgument`) runs with the profile that the argument selects.

    `agent_info` is `None` when the session metadata is not available
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
        signature = inspect.signature(fn)
        takes_profile = "profile" in signature.parameters
        takes_environment = "environment" in signature.parameters

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

            profile: Optional[str] = None
            environment: Optional[str] = None
            if takes_profile or takes_environment:
                arguments = signature.bind_partial(*args, **kwargs).arguments
                profile = arguments.get("profile")
                environment = arguments.get("environment")

            with _context_reload_lock:
                ctx_plug = Container()[PluggableRunContext]
                ctx_plug.reload(ctx_plug.context.run_dir)
                if takes_environment:
                    profile = _activate_call_profile(ctx_plug, environment, environment=True)
                elif profile is not None:
                    profile = _activate_call_profile(ctx_plug, profile, environment=False)
                if profile:
                    props["profile"] = profile
                with custom_environ({WORKSPACE__PROFILE: profile} if profile else {}):
                    try:
                        rv = fn(*args, **kwargs)
                        _track(True)
                        return rv
                    except Exception:
                        _track(False)
                        raise

        return wrapper  # type: ignore[return-value]

    return decorator
