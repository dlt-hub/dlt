import sys
import json
from pathlib import Path
from typing import List

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


# ---------------------------------------------------------------------------
# Transport mode tests: parametrize (cli_args, expected_transport)
# ---------------------------------------------------------------------------

_TRANSPORT_CASES = [
    # (id, base_cmd, extra_flags, expected_transport, expect_path)
    ("pipeline-default", ["pipeline", "fruitshop", "mcp"], [], "streamable-http", True),
    ("pipeline-sse", ["pipeline", "fruitshop", "mcp"], ["--sse"], "sse", True),
    ("workspace-default", ["workspace", "mcp"], [], "streamable-http", True),
    ("workspace-sse", ["workspace", "mcp"], ["--sse"], "sse", True),
    ("ai-mcp-default", ["ai", "mcp"], [], "streamable-http", True),
    ("ai-mcp-stdio", ["ai", "mcp"], ["--stdio"], "stdio", False),
    ("ai-mcp-sse", ["ai", "mcp"], ["--sse"], "sse", True),
    ("ai-mcp-run-default", ["ai", "mcp", "run"], [], "streamable-http", True),
    ("ai-mcp-run-stdio", ["ai", "mcp", "run"], ["--stdio"], "stdio", False),
]


@pytest.mark.parametrize(
    ("base_cmd", "extra_flags", "expected_transport", "expect_path"),
    [(c[1], c[2], c[3], c[4]) for c in _TRANSPORT_CASES],
    ids=[c[0] for c in _TRANSPORT_CASES],
)
def test_mcp_transport(
    fruitshop_pipeline_context: RunContext,
    script_runner: ScriptRunner,
    mocker: MockerFixture,
    base_cmd: List[str],
    extra_flags: List[str],
    expected_transport: str,
    expect_path: bool,
) -> None:
    mock = mocker.patch.object(FastMCP, "run")
    result = script_runner.run(["dlt", "--debug"] + base_cmd + extra_flags)
    assert result.returncode == 0
    assert mock.called
    call_kwargs = mock.call_args.kwargs
    assert call_kwargs["transport"] == expected_transport
    if expect_path:
        assert call_kwargs["path"] == "/mcp"
    else:
        assert "port" not in call_kwargs
        assert "path" not in call_kwargs


# ---------------------------------------------------------------------------
# Extra features
# ---------------------------------------------------------------------------


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
    features = init_kwargs.kwargs.get("features") or init_kwargs.kwargs.get("extra_features")
    assert "rest-api-pipeline" in features
    assert "data-exploration" in features


# ---------------------------------------------------------------------------
# Install
# ---------------------------------------------------------------------------


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
    script_runner.run(["dlt", "--debug", "ai", "mcp", "install", "--agent", "claude"])
    result = script_runner.run(["dlt", "--debug", "ai", "mcp", "install", "--agent", "claude"])
    assert result.returncode == 0
    assert "already configured" in result.stdout
