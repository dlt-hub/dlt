import sys
import json
from pathlib import Path
import pytest

# skip the whole module on python < 3.10
if sys.version_info < (3, 10):
    pytest.skip("requires Python 3.10+", allow_module_level=True)

from fastmcp import FastMCP
from pytest_console_scripts import ScriptRunner
from pytest_mock import MockerFixture

from dlt.common.runtime.run_context import RunContext
from dlt._workspace.mcp.server import WorkspaceMCP

from tests.workspace.utils import fruitshop_pipeline_context as fruitshop_pipeline_context


def test_pipeline_mcp_command(
    fruitshop_pipeline_context: RunContext, script_runner: ScriptRunner, mocker: MockerFixture
) -> None:
    mock = mocker.patch.object(FastMCP, "run")
    result = script_runner.run(["dlt", "--debug", "pipeline", "fruitshop", "mcp"])
    assert mock.called
    call_kwargs = mock.call_args.kwargs
    assert call_kwargs["transport"] == "streamable-http"
    assert call_kwargs["path"] == "/mcp"
    assert result.returncode == 0
    assert result.stdout == ""


def test_pipeline_mcp_command_sse(
    fruitshop_pipeline_context: RunContext, script_runner: ScriptRunner, mocker: MockerFixture
) -> None:
    mock = mocker.patch.object(FastMCP, "run")
    result = script_runner.run(["dlt", "--debug", "pipeline", "fruitshop", "mcp", "--sse"])
    assert mock.called
    call_kwargs = mock.call_args.kwargs
    assert call_kwargs["transport"] == "sse"
    assert call_kwargs["path"] == "/mcp"
    assert result.returncode == 0
    assert result.stdout == ""


def test_workspace_mcp_command(
    fruitshop_pipeline_context: RunContext, script_runner: ScriptRunner, mocker: MockerFixture
) -> None:
    mock = mocker.patch.object(FastMCP, "run")
    result = script_runner.run(["dlt", "--debug", "workspace", "mcp"])
    assert mock.called
    call_kwargs = mock.call_args.kwargs
    assert call_kwargs["transport"] == "streamable-http"
    assert call_kwargs["path"] == "/mcp"
    assert result.returncode == 0
    assert result.stdout == ""


def test_workspace_mcp_command_sse(
    fruitshop_pipeline_context: RunContext, script_runner: ScriptRunner, mocker: MockerFixture
) -> None:
    mock = mocker.patch.object(FastMCP, "run")
    result = script_runner.run(["dlt", "--debug", "workspace", "mcp", "--sse"])
    assert mock.called
    call_kwargs = mock.call_args.kwargs
    assert call_kwargs["transport"] == "sse"
    assert call_kwargs["path"] == "/mcp"
    assert result.returncode == 0
    assert result.stdout == ""


def test_ai_mcp_command(
    fruitshop_pipeline_context: RunContext, script_runner: ScriptRunner, mocker: MockerFixture
) -> None:
    mock = mocker.patch.object(FastMCP, "run")
    result = script_runner.run(["dlt", "--debug", "ai", "mcp"])
    assert mock.called
    call_kwargs = mock.call_args.kwargs
    assert call_kwargs["transport"] == "streamable-http"
    assert call_kwargs["path"] == "/mcp"
    assert result.returncode == 0


def test_ai_mcp_command_stdio(
    fruitshop_pipeline_context: RunContext, script_runner: ScriptRunner, mocker: MockerFixture
) -> None:
    mock = mocker.patch.object(FastMCP, "run")
    result = script_runner.run(["dlt", "--debug", "ai", "mcp", "--stdio"])
    assert mock.called
    call_kwargs = mock.call_args.kwargs
    assert call_kwargs["transport"] == "stdio"
    assert "port" not in call_kwargs
    assert "path" not in call_kwargs
    assert result.returncode == 0


def test_ai_mcp_command_sse(
    fruitshop_pipeline_context: RunContext, script_runner: ScriptRunner, mocker: MockerFixture
) -> None:
    mock = mocker.patch.object(FastMCP, "run")
    result = script_runner.run(["dlt", "--debug", "ai", "mcp", "--sse"])
    assert mock.called
    call_kwargs = mock.call_args.kwargs
    assert call_kwargs["transport"] == "sse"
    assert call_kwargs["path"] == "/mcp"
    assert result.returncode == 0


def test_ai_mcp_command_extra_features(
    fruitshop_pipeline_context: RunContext, script_runner: ScriptRunner, mocker: MockerFixture
) -> None:
    spy = mocker.spy(WorkspaceMCP, "__init__")
    mocker.patch.object(FastMCP, "run")
    result = script_runner.run(
        ["dlt", "--debug", "ai", "mcp", "--features", "rest-api-pipeline", "data-exploration"]
    )
    assert result.returncode == 0
    init_kwargs = spy.call_args
    features = init_kwargs.kwargs.get("extra_features") or init_kwargs[0][3]
    assert "rest-api-pipeline" in features
    assert "data-exploration" in features


def test_ai_mcp_run_subcommand(
    fruitshop_pipeline_context: RunContext, script_runner: ScriptRunner, mocker: MockerFixture
) -> None:
    mock = mocker.patch.object(FastMCP, "run")
    result = script_runner.run(["dlt", "--debug", "ai", "mcp", "run"])
    assert mock.called
    assert result.returncode == 0


def test_ai_mcp_run_subcommand_stdio(
    fruitshop_pipeline_context: RunContext, script_runner: ScriptRunner, mocker: MockerFixture
) -> None:
    mock = mocker.patch.object(FastMCP, "run")
    result = script_runner.run(["dlt", "--debug", "ai", "mcp", "run", "--stdio"])
    assert mock.called
    call_kwargs = mock.call_args.kwargs
    assert call_kwargs["transport"] == "stdio"
    assert "port" not in call_kwargs
    assert "path" not in call_kwargs
    assert result.returncode == 0


def test_ai_mcp_install_command(
    fruitshop_pipeline_context: RunContext, script_runner: ScriptRunner
) -> None:
    result = script_runner.run(["dlt", "--debug", "ai", "mcp", "install", "--agent", "claude"])
    assert result.returncode == 0
    assert "Installed" in result.stdout

    config_path = Path(fruitshop_pipeline_context.run_dir) / ".mcp.json"
    assert config_path.is_file()
    data = json.loads(config_path.read_text())
    server = data["mcpServers"]["dlt-workspace"]
    assert server["command"] == "uv"
    assert server["args"] == ["run", "dlt", "ai", "mcp", "run", "--stdio"]
    assert server["type"] == "stdio"


def test_ai_mcp_install_with_features(
    fruitshop_pipeline_context: RunContext, script_runner: ScriptRunner
) -> None:
    result = script_runner.run(
        [
            "dlt",
            "--debug",
            "ai",
            "mcp",
            "install",
            "--agent",
            "claude",
            "--features",
            "search",
            "--name",
            "dlt-search",
        ]
    )
    assert result.returncode == 0

    config_path = Path(fruitshop_pipeline_context.run_dir) / ".mcp.json"
    data = json.loads(config_path.read_text())
    server = data["mcpServers"]["dlt-search"]
    assert "--features" in server["args"]
    assert "search" in server["args"]


def test_ai_mcp_install_skips_existing(
    fruitshop_pipeline_context: RunContext, script_runner: ScriptRunner
) -> None:
    # first install
    script_runner.run(["dlt", "--debug", "ai", "mcp", "install", "--agent", "claude"])
    # second install — should skip
    result = script_runner.run(["dlt", "--debug", "ai", "mcp", "install", "--agent", "claude"])
    assert result.returncode == 0
    assert "already configured" in result.stdout
