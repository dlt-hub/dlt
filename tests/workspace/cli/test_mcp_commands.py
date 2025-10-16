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
    # patch mcp server
    # NOTE: patch works because we call in-process
    mock = mocker.patch.object(FastMCP, "run")
    # mcp server will be able attach to pokemon pipeline
    result = script_runner.run(["dlt", "--debug", "pipeline", "fruitshop", "mcp"])
    assert mock.called
    assert result.returncode == 0
    # can't have any stdout!
    assert result.stdout == ""


def test_workspace_mcp_command(
    fruitshop_pipeline_context: RunContext, script_runner: ScriptRunner, mocker: MockerFixture
) -> None:
    # patch mcp server
    # NOTE: patch works because we call in-process
    mock = mocker.patch.object(FastMCP, "run")
    # mcp server will be able attach to pokemon pipeline
    result = script_runner.run(["dlt", "--debug", "workspace", "mcp"])
    assert mock.called
    assert result.returncode == 0
    # can't have any stdout!
    assert result.stdout == ""
