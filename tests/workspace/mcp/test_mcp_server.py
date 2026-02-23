import asyncio
from typing import Optional, Set

from fastmcp.prompts import Prompt
from fastmcp.tools import Tool

from dlt.common.configuration import plugins
from dlt.common.configuration.plugins import manager as get_manager
from dlt.common.configuration.specs.pluggable_run_context import RunContextBase

from dlt._workspace.mcp import PipelineMCP, WorkspaceMCP
from dlt._workspace._plugins import McpFeatures, plug_mcp_pipeline, plug_mcp_workspace
from dlt._workspace.mcp.tools import data_tools

from tests.workspace.utils import pokemon_pipeline_context as pokemon_pipeline_context


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
        "display_schema",
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

    tool_names_expected = [
        "list_pipelines",
        "list_tables",
        "get_table_schema",
        "get_table_create_sql",
        "preview_table",
        "execute_sql_query",
        "get_row_counts",
        "display_schema",
        "get_local_pipeline_state",
    ]

    tool_names_actual = [tool.name for tool in tools]
    assert tool_names_actual == tool_names_expected
    assert all(tool.description for tool in tools)

    # workspace tools (except list_pipelines) should expose pipeline_name
    for tool in tools:
        if tool.name == "list_pipelines":
            continue
        param_names = list(tool.parameters.get("properties", {}).keys())
        assert (
            "pipeline_name" in param_names
        ), f"Tool {tool.name} should expose pipeline_name parameter"


def test_plug_mcp_pipeline_returns_none_for_unknown_features() -> None:
    assert plug_mcp_pipeline({"unknown"}) is None


def test_plug_mcp_pipeline_excludes_list_pipelines() -> None:
    result = plug_mcp_pipeline({"pipeline"})
    assert result is not None
    assert result.name == "pipeline"
    assert data_tools.list_pipelines not in result.tools
    assert data_tools.list_tables in result.tools


def test_plug_mcp_workspace_returns_none_for_unknown_features() -> None:
    assert plug_mcp_workspace({"unknown"}) is None


def test_plug_mcp_workspace_returns_list_pipelines() -> None:
    result = plug_mcp_workspace({"workspace"})
    assert result is not None
    assert result.name == "workspace"
    assert data_tools.list_pipelines in result.tools
    assert len(result.tools) == 1


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
