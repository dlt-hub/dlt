import contextlib
import functools
import inspect
import threading
import time
from typing import Any, Callable, ContextManager, Dict, Optional

from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs import RuntimeConfiguration
from dlt.common.configuration.specs.pluggable_run_context import PluggableRunContext
from dlt.common.runtime.anon_tracker import track
from dlt.common.runtime.telemetry import _TELEMETRY_STARTED, start_telemetry
from dlt.common.typing import TFun
from dlt.common.utils import digest128

from dlt._workspace._workspace_context import WorkspaceRunContext
from dlt._workspace.exceptions import ProfileException
from dlt._workspace.environment import use_profile_options
from dlt._workspace.typing import TProfileOptionsTakes

# serialize context reloads and tool bodies: the run context is global
_context_reload_lock = threading.Lock()

TProfileArgument = Optional[str]
"""The `profile` argument of a tool that works with a profile. The MCP server runs the call with
it, see `with_tool_run_context`, and describes it unless the tool's docstring does. Outside of a
workspace the server hides it from the tool schema."""

TEnvironmentArgument = Optional[str]
"""The `environment` argument of a tool that deploys or works with a deployment. The MCP server
runs the call with the default write profile of that environment, or with the profile that
`select_environment_profile` selects when the tool also takes `profile`. The server describes it
unless the tool's docstring does."""


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


def with_tool_run_context(fn: TFun) -> TFun:
    """Runs each call of the MCP tool `fn` on the run context reloaded from disk, so every call
    sees the latest on-disk state, with the profile that its arguments select:

    * `profile` alone: that profile, else the active one.
    * `environment` alone: the default write profile of the environment.
    * both: the profile that `select_environment_profile` selects.

    `DltMCP` wraps every tool that it serves. An argument that selects no profile raises
    `ToolError` with the remedy. After the call the reloaded run context is back, see
    `use_profile`.
    """
    from fastmcp.exceptions import ToolError

    signature = inspect.signature(fn)
    takes = _get_profile_options_takes(signature)

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        arguments: Dict[str, Any] = {}
        if takes != "none":
            arguments = signature.bind_partial(*args, **kwargs).arguments
        with _context_reload_lock, contextlib.ExitStack() as stack:
            ctx_plug = Container()[PluggableRunContext]
            ctx_plug.reload(ctx_plug.context.run_dir)
            try:
                stack.enter_context(
                    _use_call_profile(
                        ctx_plug, takes, arguments.get("environment"), arguments.get("profile")
                    )
                )
            except ProfileException as ex:
                raise ToolError(str(ex)) from ex
            return fn(*args, **kwargs)

    return wrapper  # type: ignore[return-value]


def _get_profile_options_takes(signature: inspect.Signature) -> TProfileOptionsTakes:
    """What a tool takes to select its profile: its `profile` and `environment` arguments."""
    takes_profile = "profile" in signature.parameters
    if "environment" in signature.parameters:
        return "both" if takes_profile else "environment"
    return "profile" if takes_profile else "none"


def _use_call_profile(
    ctx_plug: PluggableRunContext,
    takes: TProfileOptionsTakes,
    environment: Optional[str],
    profile: Optional[str],
) -> ContextManager[Any]:
    """The context that runs a call with the profile that its arguments select, see
    `use_profile_options`."""
    if takes == "none" or (takes == "profile" and profile is None):
        return contextlib.nullcontext()
    _check_workspace_context(ctx_plug, "profile" if takes == "profile" else "environment")
    return use_profile_options(takes, environment, profile)


def _check_workspace_context(ctx_plug: PluggableRunContext, argument: str) -> None:
    from fastmcp.exceptions import ToolError

    if not isinstance(ctx_plug.context, WorkspaceRunContext):
        raise ToolError(
            f"The `{argument}` argument needs a dltHub workspace and this MCP server runs outside"
            " of one. Run `dlthub init` in the project folder to create a workspace."
        )


def with_mcp_tool_telemetry(**extra_props: Any) -> Callable[[TFun], TFun]:
    """Telemetry decorator for MCP tool functions.

    Emits an anonymous `mcp_tool_{name}` telemetry event after each tool
    invocation, with the profile of the call for a tool that takes a `profile`
    or an `environment` argument. `agent_info` is `None` outside of an MCP
    session.

    Args:
        **extra_props: literal key-value pairs added to the telemetry event
            properties.

    Returns:
        A decorator that wraps the tool function with telemetry tracking.
    """

    def decorator(fn: TFun) -> TFun:
        event_name = "tool_" + fn.__name__
        parameters = inspect.signature(fn).parameters
        tracks_profile = "profile" in parameters or "environment" in parameters

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            props: Dict[str, Any] = {"mcp_primitive": "tool"}
            props.update(extra_props)
            props["agent_info"] = _get_agent_info()
            ctx = Container()[PluggableRunContext].context
            if tracks_profile and isinstance(ctx, WorkspaceRunContext):
                props["profile"] = ctx.profile
            start_ts = time.time()

            def _track(success: bool) -> None:
                with contextlib.suppress(Exception):
                    props["elapsed"] = time.time() - start_ts
                    props["success"] = success
                    if not _TELEMETRY_STARTED:
                        c = resolve_configuration(RuntimeConfiguration())
                        start_telemetry(c)
                    track("mcp", event_name, props)

            try:
                rv = fn(*args, **kwargs)
                _track(True)
                return rv
            except Exception:
                _track(False)
                raise

        return wrapper  # type: ignore[return-value]

    return decorator
