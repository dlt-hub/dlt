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

from tests.workspace.utils import (
    fruitshop_pipeline_context as fruitshop_pipeline_context,
    isolated_workspace,
)


_TRANSPORT_CASES = [
    # (id, host, base_cmd, extra_flags, expected_transport, expect_path)
    ("pipeline-default", "dlt", ["pipeline", "fruitshop", "mcp"], [], "streamable-http", True),
    ("pipeline-sse", "dlt", ["pipeline", "fruitshop", "mcp"], ["--sse"], "sse", True),
    ("ai-mcp-default", "dlthub", ["ai", "mcp"], [], "streamable-http", True),
    ("ai-mcp-stdio", "dlthub", ["ai", "mcp"], ["--stdio"], "stdio", False),
    ("ai-mcp-sse", "dlthub", ["ai", "mcp"], ["--sse"], "sse", True),
    ("ai-mcp-run-default", "dlthub", ["ai", "mcp", "run"], [], "streamable-http", True),
    ("ai-mcp-run-stdio", "dlthub", ["ai", "mcp", "run"], ["--stdio"], "stdio", False),
]


@pytest.mark.parametrize(
    ("host", "base_cmd", "extra_flags", "expected_transport", "expect_path"),
    [(c[1], c[2], c[3], c[4], c[5]) for c in _TRANSPORT_CASES],
    ids=[c[0] for c in _TRANSPORT_CASES],
)
def test_mcp_transport(
    fruitshop_pipeline_context: RunContext,
    script_runner: ScriptRunner,
    mocker: MockerFixture,
    monkeypatch: pytest.MonkeyPatch,
    host: str,
    base_cmd: List[str],
    extra_flags: List[str],
    expected_transport: str,
    expect_path: bool,
) -> None:
    mock = mocker.patch.object(FastMCP, "run")
    # `dlt pipeline <name> mcp` is a legacy command; with an active workspace the `dlt` host
    # falls back to `dlthub`. nest a legacy context and pass the workspace's
    # pipelines dir via `--pipelines-dir` so `dlt.attach` still finds the fruitshop pipeline
    if host == "dlt":
        # `dlt pipeline ... mcp` is gated on `dlt[hub]`; `install-workspace` doesn't pull it in
        monkeypatch.setattr("dlt.hub.__found__", True)
        pipelines_dir = fruitshop_pipeline_context.get_data_entity("pipelines")
        with isolated_workspace("legacy", required="RunContext"):
            # `--pipelines-dir` is on the `pipeline` parser, before the pipeline_name positional
            cmd = (
                [host, "--debug", base_cmd[0], "--pipelines-dir", pipelines_dir]
                + base_cmd[1:]
                + extra_flags
            )
            result = script_runner.run(cmd)
    else:
        result = script_runner.run([host, "--debug"] + base_cmd + extra_flags)
    assert result.returncode == 0
    assert mock.called
    call_kwargs = mock.call_args.kwargs
    assert call_kwargs["transport"] == expected_transport
    if expect_path:
        assert call_kwargs["path"] == "/mcp"
    else:
        assert "port" not in call_kwargs
        assert "path" not in call_kwargs


def test_ai_mcp_command_extra_features(
    fruitshop_pipeline_context: RunContext, script_runner: ScriptRunner, mocker: MockerFixture
) -> None:
    spy = mocker.spy(WorkspaceMCP, "__init__")
    mocker.patch.object(FastMCP, "run")
    result = script_runner.run(
        ["dlthub", "--debug", "ai", "mcp", "--features", "rest-api-pipeline", "data-exploration"]
    )
    assert result.returncode == 0
    init_kwargs = spy.call_args
    features = init_kwargs.kwargs.get("features") or init_kwargs.kwargs.get("extra_features")
    assert "rest-api-pipeline" in features
    assert "data-exploration" in features


def test_ai_mcp_install_command(
    fruitshop_pipeline_context: RunContext, script_runner: ScriptRunner
) -> None:
    result = script_runner.run(["dlthub", "--debug", "ai", "mcp", "install", "--agent", "claude"])
    assert result.returncode == 0
    assert "Installed" in result.stdout

    config_path = Path(fruitshop_pipeline_context.run_dir) / ".mcp.json"
    assert config_path.is_file()
    data = json.loads(config_path.read_text())
    server = data["mcpServers"]["dlt-workspace"]
    assert server["command"] == "uv"
    assert server["args"] == ["run", "dlthub", "ai", "mcp", "run", "--stdio"]
    assert server["type"] == "stdio"


def test_ai_mcp_install_with_features(
    fruitshop_pipeline_context: RunContext, script_runner: ScriptRunner
) -> None:
    result = script_runner.run(
        [
            "dlthub",
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
    script_runner.run(["dlthub", "--debug", "ai", "mcp", "install", "--agent", "claude"])
    result = script_runner.run(["dlthub", "--debug", "ai", "mcp", "install", "--agent", "claude"])
    assert result.returncode == 0
    assert "already configured" in result.stdout
