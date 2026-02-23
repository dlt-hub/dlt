import sys
import pytest

# skip the whole module on python < 3.10
if sys.version_info < (3, 10):
    pytest.skip("requires Python 3.10+", allow_module_level=True)

from mcp.server.fastmcp import FastMCP
from pytest_console_scripts import ScriptRunner
from pytest_mock import MockerFixture

from dlt.common.runtime.run_context import RunContext

from tests.workspace.utils import fruitshop_pipeline_context as fruitshop_pipeline_context


def test_pipeline_mcp_command(
    fruitshop_pipeline_context: RunContext, script_runner: ScriptRunner, mocker: MockerFixture
) -> None:
    mock = mocker.patch.object(FastMCP, "run")
    result = script_runner.run(["dlt", "--debug", "pipeline", "fruitshop", "mcp"])
    assert mock.called
    mock.assert_called_with(transport="streamable-http")
    assert result.returncode == 0
    assert result.stdout == ""


def test_pipeline_mcp_command_sse(
    fruitshop_pipeline_context: RunContext, script_runner: ScriptRunner, mocker: MockerFixture
) -> None:
    mock = mocker.patch.object(FastMCP, "run")
    result = script_runner.run(["dlt", "--debug", "pipeline", "fruitshop", "mcp", "--sse"])
    assert mock.called
    mock.assert_called_with(transport="sse")
    assert result.returncode == 0
    assert result.stdout == ""


def test_workspace_mcp_command(
    fruitshop_pipeline_context: RunContext, script_runner: ScriptRunner, mocker: MockerFixture
) -> None:
    mock = mocker.patch.object(FastMCP, "run")
    result = script_runner.run(["dlt", "--debug", "workspace", "mcp"])
    assert mock.called
    mock.assert_called_with(transport="streamable-http")
    assert result.returncode == 0
    assert result.stdout == ""


def test_workspace_mcp_command_sse(
    fruitshop_pipeline_context: RunContext, script_runner: ScriptRunner, mocker: MockerFixture
) -> None:
    mock = mocker.patch.object(FastMCP, "run")
    result = script_runner.run(["dlt", "--debug", "workspace", "mcp", "--sse"])
    assert mock.called
    mock.assert_called_with(transport="sse")
    assert result.returncode == 0
    assert result.stdout == ""
