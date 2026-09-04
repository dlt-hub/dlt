"""Tools declare the access they require, and the server serves what the grant covers."""

from typing import Any, Dict, List, Optional

import pytest

from dlt._workspace.access import (
    FULL_ACCESS,
    RequiresAccess,
    format_access,
    missing_access,
    parse_access,
    required_access,
)
from dlt._workspace.mcp.tools import context_tools, data_tools, secrets_tools, toolkit_tools
from dlt._workspace.typing import TWorkspaceAccess

from tests.workspace.utils import isolated_workspace

# every tool dlt serves, and what it needs. A new tool belongs here deliberately.
TOOL_ACCESS = {
    "list_pipelines": {"data": ["read"]},
    "list_profiles": {"local": ["read"]},
    "get_workspace_info": {"local": ["read"]},
    "list_tables": {"data": ["read"]},
    "get_table_schema": {"data": ["read"]},
    "get_table_create_sql": {"data": ["read"]},
    "preview_table": {"data": ["read"]},
    "execute_sql_query": {"data": ["read"]},
    "get_row_counts": {"data": ["read"]},
    "get_local_pipeline_state": {"data": ["read"]},
    # `save_to_file` writes wherever it is told
    "export_schema": {"data": ["read"], "local": ["write"]},
    # dlt's own catalogue, fetched into dlt's own cache: declared as needing nothing
    "list_toolkits": {},
    "toolkit_info": {},
    "secrets_list": {"local": ["read"]},
    "secrets_view_redacted": {"local": ["read"]},
    "secrets_update_fragment": {"local": ["write"]},
    "search_dlthub_sources": {"context": ["read"]},
}


def _all_tools() -> Dict[str, Any]:
    tools = (
        list(data_tools.__tools__)
        + [data_tools.get_workspace_info]
        + list(toolkit_tools.__tools__)
        + list(secrets_tools.__tools__)
        + list(context_tools.__tools__)
    )
    return {tool.__name__: tool for tool in tools}


def test_every_tool_declares_what_it_needs() -> None:
    served = _all_tools()
    assert set(served) == set(TOOL_ACCESS), "a tool was added or removed without an access decision"
    assert {name: required_access(tool) for name, tool in served.items()} == TOOL_ACCESS


@pytest.mark.parametrize(
    "grant,served,pruned",
    [
        (
            "data:read",
            ["preview_table", "execute_sql_query", "list_toolkits"],
            ["secrets_update_fragment", "export_schema", "secrets_list", "search_dlthub_sources"],
        ),
        (
            "local:read,local:write",
            ["secrets_list", "secrets_update_fragment", "get_workspace_info"],
            ["preview_table", "export_schema", "search_dlthub_sources"],
        ),
        ("local:all,data:all,context:read", list(TOOL_ACCESS), []),
        ("", ["list_toolkits", "toolkit_info"], ["list_pipelines", "secrets_list"]),
    ],
    ids=["data-read", "local-read-write", "everything", "nothing"],
)
def test_access_decides_which_tools_are_served(
    grant: str, served: List[str], pruned: List[str]
) -> None:
    granted = parse_access(grant)
    assert granted is not None
    for name in served:
        assert not missing_access(required_access(_all_tools()[name]), granted), name
    for name in pruned:
        assert missing_access(required_access(_all_tools()[name]), granted), name


def test_undeclared_access_serves_everything() -> None:
    """A person at a terminal declares nothing, and `dlthub ai mcp run` keeps every tool."""
    assert parse_access(None) is None


@pytest.mark.parametrize(
    "text,expected",
    [
        ("data:read", {"data": ["read"]}),
        ("data:read,local:write", {"data": ["read"], "local": ["write"]}),
        ("local:read,local:write", {"local": ["read", "write"]}),
        ("toolkits,context:read", {"toolkits": True, "context": ["read"]}),
        ("", {}),
    ],
    ids=["one", "two-axes", "two-verbs", "toolkits", "empty"],
)
def test_access_round_trips_through_the_cli_argument(text: str, expected: Any) -> None:
    parsed = parse_access(text)
    assert parsed == expected
    assert parse_access(format_access(parsed)) == expected


@pytest.mark.parametrize(
    "text", ["data", "nonsense:read", "data:"], ids=["no-verb", "axis", "bare"]
)
def test_malformed_access_is_refused(text: str) -> None:
    with pytest.raises(ValueError, match="access value"):
        parse_access(text)


def test_requires_access_reads_through_annotations() -> None:
    from dlt.common.typing import Annotated

    def tool() -> Annotated[str, RequiresAccess(data=["read"], local=["write"])]:
        return ""

    def needs_nothing() -> Annotated[str, RequiresAccess()]:
        return ""

    assert required_access(tool) == {"data": ["read"], "local": ["write"]}
    assert required_access(needs_nothing) == {}


def test_an_undeclared_tool_is_assumed_to_need_everything() -> None:
    """A tool that declares nothing needs everything, so no limited caller is served it."""

    def undeclared() -> str:
        return ""

    assert required_access(undeclared) == FULL_ACCESS
    assert missing_access(required_access(undeclared), parse_access("data:read") or {})
    # and it is served when the caller was granted everything
    assert not missing_access(required_access(undeclared), FULL_ACCESS)


def test_stdio_args_carry_the_access() -> None:
    from dlt._workspace.cli.dlthub.ai.utils import mcp_stdio_args

    access: TWorkspaceAccess = {"data": ["read"]}
    args = mcp_stdio_args(["pipeline"], with_defaults=False, access=access)

    assert args[:4] == ["ai", "mcp", "run", "--stdio"]
    assert "--access" in args and args[args.index("--access") + 1] == "data:read"
    # the server parses back exactly what the loop granted
    assert parse_access(args[args.index("--access") + 1]) == access


@pytest.mark.parametrize(
    "grant,expected",
    [(None, 17), ("data:read", 10), ("local:write", 3), ("", 2)],
    ids=["ungranted", "data-read", "local-write", "nothing"],
)
def test_the_server_registers_only_what_it_serves(grant: Optional[str], expected: int) -> None:
    """Pruning happens before `add_tool`, so a tool the grant misses does not exist."""
    from dlt._workspace.mcp.server import WorkspaceMCP

    with isolated_workspace("empty", profile="dev"):
        server = WorkspaceMCP("dlt", access=parse_access(grant))
    served = {tool.name for tool in server._local_provider._components.values()}

    assert len(served) == expected
    if grant == "data:read":
        assert "preview_table" in served and "secrets_update_fragment" not in served
