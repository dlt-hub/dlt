from pathlib import Path
from typing import Dict, Optional

from pydantic import Field
from fastmcp.exceptions import ToolError

from dlt.common.runtime import run_context
from dlt.common.typing import Annotated
from dlt._workspace.cli.dlthub.ai.utils import (
    fetch_workbench_base,
    fetch_workbench_toolkit_info,
    fetch_workbench_toolkits,
    is_toolkit_visible,
    resolve_harness_source,
)
from dlt._workspace.cli.dlthub.ai.typing import TToolkitInfo, TWorkbenchToolkitInfo
from dlt._workspace.access import RequiresAccess
from dlt._workspace.mcp.context import with_mcp_tool_telemetry


def _agent_name() -> Optional[str]:
    from dlt._workspace.cli.dlthub.ai.agents import _AIAgent

    return _AIAgent.detect_agent_name(None, Path(run_context.active().run_dir))


@with_mcp_tool_telemetry()
def list_toolkits() -> Annotated[Dict[str, TToolkitInfo], RequiresAccess()]:
    """List the dlt AI toolkits available to the AI coding agent, with names and descriptions."""
    try:
        base = fetch_workbench_base(*resolve_harness_source())
    except FileNotFoundError as ex:
        raise ToolError(str(ex))
    toolkits, _ = fetch_workbench_toolkits(base, listed_only=True, agent_name=_agent_name())
    return toolkits


@with_mcp_tool_telemetry()
def toolkit_info(
    name: Annotated[str, Field(description="Name of the toolkit to inspect")],
) -> Annotated[TWorkbenchToolkitInfo, RequiresAccess()]:
    """Show detailed contents of a dlt AI toolkit."""
    agent_name = _agent_name()
    try:
        info = fetch_workbench_toolkit_info(name, *resolve_harness_source(), agent_name=agent_name)
    except (FileNotFoundError, ValueError) as ex:
        raise ToolError(str(ex))
    if info is None:
        raise ToolError("Toolkit '%s' not found" % name)
    if not is_toolkit_visible(info, agent_name):
        raise ToolError("Toolkit '%s' is not available for %s" % (name, agent_name))
    return info


__tools__ = (list_toolkits, toolkit_info)
