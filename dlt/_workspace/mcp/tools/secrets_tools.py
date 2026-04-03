from typing import Any, Dict, List, Optional

from pydantic import Field
from fastmcp.exceptions import ToolError

from dlt.common.typing import Annotated
from dlt._workspace.cli.ai.utils import (
    fetch_secrets_list,
    fetch_secrets_update_fragment,
    fetch_secrets_view_redacted,
)
from dlt._workspace.mcp.context import with_mcp_tool_telemetry


@with_mcp_tool_telemetry()
def secrets_list() -> List[Dict[str, Any]]:
    """List secret file paths, profiles, and whether each file exists."""

    return fetch_secrets_list()  # type: ignore[return-value]


@with_mcp_tool_telemetry()
def secrets_view_redacted(
    path: Annotated[
        Optional[str],
        Field(
            description=(
                "Absolute path to a specific secrets file (from secrets_list)."
                " Omit to get the unified merged view of all secret files."
            )
        ),
    ] = None,
) -> str:
    """Show secrets TOML with every value replaced by '***'.

    Without path: returns the unified merged view across all project secret files
    (read-only, shows effective configuration). With path: returns that single file
    redacted. Use secrets_list to discover file paths.
    """
    result = fetch_secrets_view_redacted(path)
    if result is None:
        if path:
            raise ToolError("Secrets file not found: %s" % path)
        raise ToolError("No secrets found in project providers.")
    return result


@with_mcp_tool_telemetry()
def secrets_update_fragment(
    fragment: Annotated[
        str,
        Field(description="TOML fragment to deep-merge into the secrets file"),
    ],
    path: Annotated[
        str,
        Field(description="Absolute path to the secrets file to update (from secrets_list)."),
    ],
) -> str:
    """Deep-merge a TOML fragment into a secrets file; returns the redacted result. The file is created if it does not exist."""
    try:
        return fetch_secrets_update_fragment(fragment, path)
    except Exception as ex:
        raise ToolError("Invalid TOML fragment: %s" % str(ex)) from ex


__tools__ = (secrets_list, secrets_view_redacted, secrets_update_fragment)
