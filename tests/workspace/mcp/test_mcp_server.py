import asyncio

import dlt
from dlt.common.configuration.specs.pluggable_run_context import RunContextBase

from dlt._workspace.mcp import PipelineMCP, WorkspaceMCP
from dlt._workspace.mcp.tools import PipelineMCPTools

from tests.workspace.utils import pokemon_pipeline_context as pokemon_pipeline_context


def test_pipeline_mcp_server(pokemon_pipeline_context: RunContextBase) -> None:
    pipeline_name = "rest_api_pokemon"
    pipeline = dlt.attach(pipeline_name)

    # TODO: also test PipelineMCPConfiguration when implemented
    mcp = PipelineMCP(pipeline)
    tools = asyncio.run(mcp.list_tools())

    tool_names_expected = [
        "available_tables",
        "table_head",
        "table_schema",
        "query_sql",
        "bookmark_sql",
        "read_result_from_bookmark",
        "recent_result",
    ]

    tool_names_actual = [tool.name for tool in tools]
    assert tool_names_actual == tool_names_expected
    assert all(tool.description for tool in tools)


def test_workspace_mcp_server(pokemon_pipeline_context: RunContextBase) -> None:
    # TODO: also test PipelineMCPConfiguration when implemented
    mcp = WorkspaceMCP(pokemon_pipeline_context.name)

    tools = asyncio.run(mcp.list_tools())

    # print([tool.name for tool in tools])

    tool_names_expected = [
        "available_pipelines",
        "available_tables",
        "table_preview",
        "table_schema",
        "execute_sql_query",
    ]

    tool_names_actual = [tool.name for tool in tools]
    assert tool_names_actual == tool_names_expected
    assert all(tool.description for tool in tools)
