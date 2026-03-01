from typing import Any, Dict, List, Optional

from pydantic import Field
from fastmcp.exceptions import ToolError

from dlt.common.typing import Annotated
from dlt._workspace.cli.ai.utils import (
    fetch_secrets_list,
    fetch_secrets_update_fragment,
    fetch_secrets_view_redacted,
)
from dlt._workspace.mcp.tools._context import with_mcp_tool_telemetry


@with_mcp_tool_telemetry()
def secrets_list() -> List[Dict[str, Any]]:
    """List project-scoped secret file locations with profile and presence info.

    Returns locations sorted with profile-scoped files first. Use this to
    discover which secrets files exist before reading or updating them."""
    return fetch_secrets_list()  # type: ignore[return-value]


@with_mcp_tool_telemetry()
def secrets_view_redacted(
    path: Annotated[
        Optional[str],
        Field(
            description=(
                "Path to a specific secrets file to view. "
                "Omit to see the unified merged view from all project secret files."
            )
        ),
    ] = None,
) -> str:
    """View secrets TOML with all values replaced by '***'.

    Without path, shows the unified merged view from all project secret files
    (same view dlt uses internally). With path, shows that exact file redacted.
    Use this to inspect which credentials are configured without exposing values."""
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
        Field(
            description=(
                "Path to the secrets file to update. "
                "Use secrets_list to discover available paths."
            )
        ),
    ],
) -> str:
    """Merge a TOML fragment into a secrets file and return the redacted result.

    Creates the file if needed. Deep-merges without overwriting other sections.
    Use secrets_list to find the target file, then pass it as path."""
    try:
        return fetch_secrets_update_fragment(fragment, path)
    except Exception as ex:
        raise ToolError("Invalid TOML fragment: %s" % str(ex)) from ex


__tools__ = (secrets_list, secrets_view_redacted, secrets_update_fragment)
