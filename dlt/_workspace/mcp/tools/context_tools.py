from typing import List

from pydantic import Field

from dlt.common.typing import Annotated
from dlt._workspace.cli._ai_context_api_client import search_sources
from dlt._workspace.cli.exceptions import ScaffoldApiError
from dlt._workspace.mcp.context import with_mcp_tool_telemetry
from dlt._workspace.typing import TSourceItem

from fastmcp.exceptions import ToolError


@with_mcp_tool_telemetry()
def search_dlthub_sources(
    query: Annotated[
        str,
        Field(
            description=(
                "Search term to find sources by name or description. Empty string returns all"
                " sources alphabetically."
            )
        ),
    ] = "",
) -> List[TSourceItem]:
    """Search for available dlt sources on dlthub by name or description."""
    try:
        return search_sources(query=query)
    except ScaffoldApiError as ex:
        raise ToolError(str(ex))


__tools__ = (search_dlthub_sources,)
