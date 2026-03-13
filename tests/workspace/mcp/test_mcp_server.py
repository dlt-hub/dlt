import asyncio
import os
from typing import Any, Callable, List, Optional, Set
from unittest.mock import Mock, patch

import pytest
from pytest_mock import MockerFixture
from fastmcp.prompts import Prompt
from fastmcp.tools import Tool

from dlt.common.configuration import plugins
from dlt.common.configuration.container import Container
from dlt.common.configuration.providers.toml import SecretsTomlProvider
from dlt.common.configuration.plugins import manager as get_manager
from dlt.common.configuration.specs.pluggable_run_context import RunContextBase, PluggableRunContext
from dlt.common.runtime.anon_tracker import disable_anon_tracker
from dlt.common.typing import DictStrAny

from dlt._workspace.mcp import PipelineMCP, WorkspaceMCP
from dlt._workspace.mcp.server import resolve_features
from dlt._workspace._plugins import (
    McpFeatures,
    plug_mcp_context,
    plug_mcp_pipeline,
    plug_mcp_secrets,
    plug_mcp_toolkit,
    plug_mcp_workspace,
)
from dlt._workspace.configuration import WorkspaceRuntimeConfiguration
from dlt._workspace.mcp.tools import secrets_tools

from tests.common.runtime.utils import mock_github_env, mock_pod_env
from tests.utils import (
    disable_temporary_telemetry as disable_temporary_telemetry,
    start_test_telemetry,
)
from tests.workspace.utils import (
    isolated_workspace,
    pokemon_pipeline_context as pokemon_pipeline_context,
)


def test_pipeline_mcp_server(pokemon_pipeline_context: RunContextBase) -> None:
    pipeline_name = "rest_api_pokemon"

    mcp = PipelineMCP(pipeline_name)
    tools = asyncio.run(mcp.list_tools())

    tool_names_expected = [
        "list_tables",
        "get_table_schema",
        "get_table_create_sql",
        "preview_table",
        "execute_sql_query",
        "get_row_counts",
        "export_schema",
        "get_local_pipeline_state",
    ]

    tool_names_actual = [tool.name for tool in tools]
    assert tool_names_actual == tool_names_expected
    assert all(tool.description for tool in tools)

    # pipeline_name should NOT be exposed as a parameter
    for tool in tools:
        param_names = list(tool.parameters.get("properties", {}).keys())
        assert (
            "pipeline_name" not in param_names
        ), f"Tool {tool.name} should not expose pipeline_name parameter"


def test_workspace_mcp_server(pokemon_pipeline_context: RunContextBase) -> None:
    mcp = WorkspaceMCP(pokemon_pipeline_context.name)
    tools = asyncio.run(mcp.list_tools())

    tool_names_actual = [tool.name for tool in tools]
    # workspace tools: list_pipelines, list_profiles, get_workspace_info
    assert "list_pipelines" in tool_names_actual
    assert "list_profiles" in tool_names_actual
    assert "get_workspace_info" in tool_names_actual
    # toolkit tools: list_toolkits, toolkit_info
    assert "list_toolkits" in tool_names_actual
    assert "toolkit_info" in tool_names_actual
    # secrets tools
    assert "secrets_list" in tool_names_actual
    assert "secrets_view_redacted" in tool_names_actual
    assert "secrets_update_fragment" in tool_names_actual
    # context tools
    assert "search_dlthub_sources" in tool_names_actual
    # pipeline tools
    for pipeline_tool in [
        "list_tables",
        "get_table_schema",
        "get_table_create_sql",
        "preview_table",
        "execute_sql_query",
        "get_row_counts",
        "export_schema",
        "get_local_pipeline_state",
    ]:
        assert pipeline_tool in tool_names_actual
    assert all(tool.description for tool in tools)

    # tools that should NOT expose pipeline_name
    no_pipeline_name = {
        "list_pipelines",
        "list_profiles",
        "get_workspace_info",
        "list_toolkits",
        "toolkit_info",
        "secrets_list",
        "secrets_view_redacted",
        "secrets_update_fragment",
        "search_dlthub_sources",
    }
    # pipeline-scoped tools should expose pipeline_name
    for tool in tools:
        param_names = list(tool.parameters.get("properties", {}).keys())
        if tool.name in no_pipeline_name:
            assert (
                "pipeline_name" not in param_names
            ), f"Tool {tool.name} should not expose pipeline_name"
        else:
            assert (
                "pipeline_name" in param_names
            ), f"Tool {tool.name} should expose pipeline_name parameter"


@pytest.mark.parametrize(
    "hook_fn,feature",
    [
        (plug_mcp_pipeline, "pipeline"),
        (plug_mcp_workspace, "workspace"),
        (plug_mcp_toolkit, "toolkit"),
        (plug_mcp_secrets, "secrets"),
        (plug_mcp_context, "context"),
    ],
)
def test_plug_mcp_returns_none_for_unknown_features(
    hook_fn: Callable[[Set[str]], Optional[McpFeatures]], feature: str
) -> None:
    assert hook_fn({"unknown"}) is None


@pytest.mark.parametrize(
    "hook_fn,feature,expected_name,expected_tool_names",
    [
        (
            plug_mcp_pipeline,
            "pipeline",
            "pipeline",
            [
                "list_tables",
                "get_table_schema",
                "get_table_create_sql",
                "preview_table",
                "execute_sql_query",
                "get_row_counts",
                "export_schema",
                "get_local_pipeline_state",
            ],
        ),
        (
            plug_mcp_workspace,
            "workspace",
            "workspace",
            ["list_pipelines", "list_profiles", "get_workspace_info"],
        ),
        (
            plug_mcp_toolkit,
            "toolkit",
            "toolkit",
            ["list_toolkits", "toolkit_info"],
        ),
        (
            plug_mcp_secrets,
            "secrets",
            "secrets",
            ["secrets_list", "secrets_view_redacted", "secrets_update_fragment"],
        ),
        (
            plug_mcp_context,
            "context",
            "context",
            ["search_dlthub_sources"],
        ),
    ],
)
def test_plug_mcp_returns_expected_tools(
    hook_fn: Callable[[Set[str]], Optional[McpFeatures]],
    feature: str,
    expected_name: str,
    expected_tool_names: List[str],
) -> None:
    result = hook_fn({feature})
    assert result is not None
    assert result.name == expected_name
    actual_names = [t.__name__ for t in result.tools]
    assert actual_names == expected_tool_names


def test_plug_mcp_pipeline_excludes_workspace_tools() -> None:
    """Pipeline feature must not include list_pipelines or list_profiles."""
    result = plug_mcp_pipeline({"pipeline"})
    assert result is not None
    tool_names = [t.__name__ for t in result.tools]
    assert "list_pipelines" not in tool_names
    assert "list_profiles" not in tool_names


def test_register_tool_and_prompt_objects(pokemon_pipeline_context: RunContextBase) -> None:
    """Tools and prompts can be passed as Tool/Prompt objects, not just functions."""

    def extra_tool(x: str) -> str:
        """An extra tool."""
        return x

    def extra_prompt() -> str:
        """An extra prompt."""
        return "hello"

    tool_obj = Tool.from_function(extra_tool)
    prompt_obj = Prompt.from_function(extra_prompt)

    class _ObjPlugin:
        @plugins.hookimpl(specname="plug_mcp")
        def plug_mcp_obj(self, features: Set[str]) -> Optional[McpFeatures]:
            if "pipeline" not in features:
                return None
            return McpFeatures(name="obj-plugin", tools=[tool_obj], prompts=[prompt_obj])

    plugin = _ObjPlugin()
    m = get_manager()
    m.register(plugin, name="test-obj-plugin")
    try:
        mcp = WorkspaceMCP("test")
        tools = asyncio.run(mcp.list_tools())
        prompts = asyncio.run(mcp.list_prompts())
        tool_names = [t.name for t in tools]
        prompt_names = [p.name for p in prompts]
        assert "extra_tool" in tool_names
        assert "extra_prompt" in prompt_names
    finally:
        m.unregister(name="test-obj-plugin")


@pytest.mark.parametrize(
    "tokens,expected_in,expected_not_in",
    [
        (None, {"pipeline", "workspace", "toolkit", "secrets"}, set()),
        ([], {"pipeline", "workspace", "toolkit", "secrets"}, set()),
        (["-secrets", "+context"], {"pipeline", "workspace", "toolkit", "context"}, {"secrets"}),
        (["context"], {"pipeline", "workspace", "toolkit", "secrets", "context"}, set()),
        (["-secrets,+context"], {"pipeline", "workspace", "toolkit", "context"}, {"secrets"}),
    ],
    ids=["none", "empty", "add-remove", "plain-add", "comma-separated"],
)
def test_resolve_features(
    tokens: Optional[List[str]],
    expected_in: Set[str],
    expected_not_in: Set[str],
) -> None:
    result = resolve_features(tokens)
    for f in expected_in:
        assert f in result, f"{f} should be in {result}"
    for f in expected_not_in:
        assert f not in result, f"{f} should not be in {result}"


def test_resolve_features_cli_argparse() -> None:
    """Prove that argparse can parse --features=-secrets,+context."""
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--features", nargs="*", default=None)
    # use = form so argparse doesn't treat -secrets as a flag
    args = parser.parse_args(["--features=-secrets,+context"])
    result = resolve_features(args.features)
    assert "secrets" not in result
    assert "context" in result
    assert "pipeline" in result
    assert "workspace" in result
    assert "toolkit" in result


@pytest.mark.parametrize(
    "workspace_name,profile,required",
    [
        ("empty", "dev", "WorkspaceRunContext"),
        ("legacy", None, "RunContext"),
    ],
    ids=["workspace", "oss"],
)
def test_secrets_update_then_merged_view_reflects_change(
    autouse_test_storage: None,
    preserve_run_context: None,
    workspace_name: str,
    profile: Optional[str],
    required: str,
) -> None:
    """After secrets_update_fragment writes a key, the merged view (no path)
    must include that key. Calls MCP tool functions which use with_reload_context."""

    with isolated_workspace(workspace_name, profile=profile, required=required):
        os.makedirs(".dlt", exist_ok=True)

        # 1) merged view before update — should be empty or raise ToolError
        try:
            before = secrets_tools.secrets_view_redacted()
        except Exception:
            before = None
        if before is not None:
            assert "api_key" not in before

        # 2) find the target path from the actual SecretsTomlProvider locations
        # (secrets_list only returns project-scoped locations, which may be empty
        # in OSS context where test conftest redirects providers to tests/.dlt)
        locations = secrets_tools.secrets_list()
        if locations:
            target_path = locations[0]["path"]
        else:
            # get the path directly from the provider
            providers = Container()[PluggableRunContext].providers.providers
            secrets_provider = next(p for p in providers if isinstance(p, SecretsTomlProvider))
            target_path = secrets_provider.locations[0]

        # 3) write a new secret via the MCP tool
        fragment = '[sources.my_source]\napi_key = "sk-test-12345"\n'
        result = secrets_tools.secrets_update_fragment(fragment, target_path)
        assert "***" in result
        assert "sk-test-12345" not in result

        # 4) merged view after update — must see the new section
        after = secrets_tools.secrets_view_redacted()
        assert after is not None, "Merged view returned None after update"
        assert "[sources.my_source]" in after
        assert "api_key" in after
        assert "sk-test-12345" not in after
        assert "***" in after


SENT_ITEMS: list[DictStrAny] = []


def _mock_before_send(event: DictStrAny, _unused_hint: Any = None) -> DictStrAny:
    SENT_ITEMS.append(event)
    return event


def test_mcp_tool_telemetry_emits_event(
    autouse_test_storage: None,
    preserve_run_context: None,
    mocker: MockerFixture,
    disable_temporary_telemetry: None,
) -> None:
    """MCP tools emit mcp_tool_<name> telemetry with primitive, elapsed, success."""
    mock_github_env(os.environ)
    mock_pod_env(os.environ)
    SENT_ITEMS.clear()
    config = WorkspaceRuntimeConfiguration(dlthub_telemetry=True)

    with patch("dlt.common.runtime.anon_tracker.before_send", _mock_before_send):
        start_test_telemetry(config)
        mocker.patch(
            "dlt.common.runtime.anon_tracker.requests.post",
            return_value=Mock(status_code=204),
        )
        with isolated_workspace("empty", profile="dev"):
            secrets_tools.secrets_list()
        disable_anon_tracker()

    assert len(SENT_ITEMS) == 1
    event = SENT_ITEMS[0]
    assert event["event"] == "mcp_tool_secrets_list"
    props = event["properties"]
    assert props["event_category"] == "mcp"
    assert props["event_name"] == "tool_secrets_list"
    assert props["mcp_primitive"] == "tool"
    assert props["success"] is True
    assert isinstance(props["elapsed"], (int, float))
    # agent_info is None when called outside a real MCP session
    assert "agent_info" in props
    assert props["agent_info"] is None
