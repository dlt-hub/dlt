"""Tests for job launchers."""

import json
import os
import subprocess
import sys
from unittest.mock import MagicMock, patch

import pytest

from dlt._workspace.deployment.launchers.job import run as job_run
from dlt._workspace.deployment.typing import TRuntimeEntryPoint

WORKSPACE = "tests.workspace.cases.runtime_workspace"


def _entry(
    module: str,
    function: str = None,
    job_type: str = "batch",
    **run_args: object,
) -> TRuntimeEntryPoint:
    """Helper to build TRuntimeEntryPoint dicts for tests."""
    ep: TRuntimeEntryPoint = {"module": module, "function": function, "job_type": job_type}  # type: ignore[typeddict-item]
    if run_args:
        ep["run_args"] = run_args  # type: ignore[typeddict-item]
    return ep


def test_job_launcher_sync() -> None:
    """Job launcher executes sync JobFactory and returns result."""
    result = job_run(
        _entry(f"{WORKSPACE}.batch_jobs", "backfill"),
        run_id="test-1",
        trigger="manual:",
    )
    assert result == "backfill_done"


def test_job_launcher_run_context_injection() -> None:
    """Job with run_context parameter receives run_id and trigger."""
    result = job_run(
        _entry(f"{WORKSPACE}.batch_jobs", "context_aware"),
        run_id="ctx-test-1",
        trigger="manual:jobs.batch_jobs.context_aware",
    )
    assert "run_id=ctx-test-1" in result
    assert "trigger=manual:jobs.batch_jobs.context_aware" in result


def test_job_launcher_run_context_not_injected() -> None:
    """Job without run_context parameter works normally."""
    result = job_run(
        _entry(f"{WORKSPACE}.batch_jobs", "backfill"),
        run_id="ctx-test-2",
        trigger="manual:",
    )
    assert result == "backfill_done"


def test_job_launcher_run_context_with_default() -> None:
    """Job with run_context=None default gets context injected by launcher."""
    result = job_run(
        _entry(f"{WORKSPACE}.batch_jobs", "context_optional"),
        run_id="ctx-test-3",
        trigger="manual:",
    )
    assert "got_context:ctx-test-3" in result


def test_job_launcher_with_config() -> None:
    """Job launcher injects config via env vars."""
    result = job_run(
        _entry(f"{WORKSPACE}.batch_jobs", "maintenance"),
        run_id="test-2",
        trigger="manual:",
        config={"cleanup_days": "30"},
    )
    assert result is None


def test_job_launcher_function_not_found() -> None:
    with pytest.raises(ImportError, match="cannot resolve"):
        job_run(
            _entry(f"{WORKSPACE}.batch_jobs", "nonexistent"),
            run_id="test-3",
            trigger="manual:",
        )


def test_job_launcher_module_not_found() -> None:
    with pytest.raises((ImportError, ModuleNotFoundError)):
        job_run(
            _entry("nonexistent.module", "foo"),
            run_id="test-4",
            trigger="manual:",
        )


def test_job_launcher_requires_function() -> None:
    with pytest.raises(ValueError, match="function"):
        job_run(
            _entry(f"{WORKSPACE}.batch_jobs"),
            run_id="test-5",
            trigger="manual:",
        )


def test_job_launcher_mcp_fallback() -> None:
    """Job returning FastMCP instance delegates to MCP launcher."""
    mock_mcp = MagicMock()
    mock_mcp.__class__.__name__ = "FastMCP"

    entry_point = _entry("test_module", "my_job", port=5000)

    with (
        patch("dlt._workspace.deployment.launchers.job._resolve_job") as mock_resolve,
        patch("dlt._workspace.deployment.launchers.job.set_config_env_vars"),
        patch("fastmcp.FastMCP", new=type(mock_mcp), create=True),
        patch("dlt._workspace.deployment.launchers.mcp.run_mcp_instance") as mock_run_mcp,
    ):
        mock_job = MagicMock()
        mock_job.section = "test"
        mock_job.name = "my_job"
        mock_job.return_value = mock_mcp
        mock_resolve.return_value = mock_job

        job_run(entry_point, run_id="test-6", trigger="manual:")
        mock_run_mcp.assert_called_once()


def test_module_launcher_builds_correct_args() -> None:
    """Module launcher calls os.execvp with uv run python -m <module>."""
    with patch("os.execvp") as mock_exec:
        from dlt._workspace.deployment.launchers.module import run

        run(f"{WORKSPACE}.etl_script", config={})
        mock_exec.assert_called_once_with(
            "uv",
            ["uv", "run", "python", "-m", f"{WORKSPACE}.etl_script"],
        )


def test_marimo_launcher_builds_correct_args() -> None:
    """Marimo launcher calls os.execvp with uv run marimo run and correct flags."""
    entry_point = _entry(f"{WORKSPACE}.marimo_notebook", port=5000)
    with (
        patch("os.execvp") as mock_exec,
        patch(
            "dlt._workspace.deployment.launchers.marimo.resolve_module_path",
            return_value="/path/to/notebook.py",
        ),
    ):
        from dlt._workspace.deployment.launchers.marimo import run

        run(entry_point, config={})

        args = mock_exec.call_args[0]
        assert args[0] == "uv"
        cmd = args[1]
        assert cmd[:3] == ["uv", "run", "marimo"]
        assert cmd[3] == "run"
        assert "/path/to/notebook.py" in cmd
        assert "--port" in cmd
        assert "5000" in cmd
        assert "--host" in cmd
        assert "0.0.0.0" in cmd
        assert "--headless" in cmd
        assert "--no-token" in cmd


def test_marimo_launcher_with_token() -> None:
    """Marimo launcher passes --token and --token-password when configured."""
    entry_point = _entry(f"{WORKSPACE}.marimo_notebook", port=5000)
    with (
        patch("os.execvp") as mock_exec,
        patch(
            "dlt._workspace.deployment.launchers.marimo.resolve_module_path",
            return_value="/path/to/notebook.py",
        ),
    ):
        os.environ["JOBS__MARIMO_NOTEBOOK__MARIMO__TOKEN"] = "my-secret"
        try:
            from dlt._workspace.deployment.launchers.marimo import run

            run(entry_point, config={})
        finally:
            del os.environ["JOBS__MARIMO_NOTEBOOK__MARIMO__TOKEN"]

        cmd = mock_exec.call_args[0][1]
        assert "--token" in cmd
        assert "--token-password" in cmd
        assert "my-secret" in cmd


def test_marimo_launcher_with_base_path() -> None:
    """Marimo launcher passes --base-url from run_args.base_path."""
    entry_point = _entry(f"{WORKSPACE}.marimo_notebook", port=5000, base_path="/workspace/123/nb")
    with (
        patch("os.execvp") as mock_exec,
        patch(
            "dlt._workspace.deployment.launchers.marimo.resolve_module_path",
            return_value="/path/to/notebook.py",
        ),
    ):
        from dlt._workspace.deployment.launchers.marimo import run

        run(entry_point, config={})

        cmd = mock_exec.call_args[0][1]
        assert "--base-url" in cmd
        assert "/workspace/123/nb" in cmd


def test_streamlit_launcher_builds_correct_args() -> None:
    """Streamlit launcher calls os.execvp with uv run streamlit and correct flags."""
    entry_point = _entry(f"{WORKSPACE}.streamlit_app", port=8501)
    with (
        patch("os.execvp") as mock_exec,
        patch(
            "dlt._workspace.deployment.launchers.streamlit.resolve_module_path",
            return_value="/path/to/app.py",
        ),
    ):
        from dlt._workspace.deployment.launchers.streamlit import run

        run(entry_point, config={})

        args = mock_exec.call_args[0]
        assert args[0] == "uv"
        cmd = args[1]
        assert cmd[:4] == ["uv", "run", "streamlit", "run"]
        assert "/path/to/app.py" in cmd
        assert "--server.address=0.0.0.0" in cmd
        assert "--server.port=8501" in cmd
        assert "--server.headless=true" in cmd
        assert "--server.enableCORS=false" in cmd
        assert "--server.enableXsrfProtection=false" in cmd
        assert "--browser.gatherUsageStats=false" in cmd


def test_mcp_launcher_calls_run() -> None:
    """MCP launcher finds FastMCP instance and calls run()."""
    entry_point = _entry(f"{WORKSPACE}.mcp_server", port=5000)
    with patch("fastmcp.FastMCP.run") as mock_run:
        from dlt._workspace.deployment.launchers.mcp import run

        run(entry_point, config={})

        mock_run.assert_called_once()
        call_kwargs = mock_run.call_args[1]
        assert call_kwargs["transport"] == "http"
        assert call_kwargs["host"] == "0.0.0.0"
        assert call_kwargs["port"] == 5000
        assert call_kwargs["path"] == "/mcp"


def test_mcp_config_override() -> None:
    """MCP config resolves from env vars."""
    entry_point = _entry(f"{WORKSPACE}.mcp_server", port=5000)
    os.environ["JOBS__MCP_SERVER__MCP__TRANSPORT"] = "streamable-http"
    os.environ["JOBS__MCP_SERVER__MCP__PATH"] = "/custom"
    os.environ["JOBS__MCP_SERVER__MCP__STATELESS_HTTP"] = "true"
    try:
        with patch("fastmcp.FastMCP.run") as mock_run:
            from dlt._workspace.deployment.launchers.mcp import run

            run(entry_point, config={})

            call_kwargs = mock_run.call_args[1]
            assert call_kwargs["transport"] == "streamable-http"
            assert call_kwargs["path"] == "/custom"
            assert call_kwargs["stateless_http"] is True
    finally:
        del os.environ["JOBS__MCP_SERVER__MCP__TRANSPORT"]
        del os.environ["JOBS__MCP_SERVER__MCP__PATH"]
        del os.environ["JOBS__MCP_SERVER__MCP__STATELESS_HTTP"]


def test_launcher_fails_without_port() -> None:
    """Interactive launchers fail if run_args.port is not provided."""
    entry_point = _entry(f"{WORKSPACE}.mcp_server")  # no port in run_args
    with pytest.raises(ValueError, match="run_args.port"):
        with patch("fastmcp.FastMCP.run"):
            from dlt._workspace.deployment.launchers.mcp import run

            run(entry_point, config={})


def test_job_launcher_via_cli() -> None:
    """Job launcher works via python -m with CLI args."""
    entry_point = json.dumps(
        {
            "module": f"{WORKSPACE}.batch_jobs",
            "function": "backfill",
            "job_type": "batch",
        }
    )
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "dlt._workspace.deployment.launchers.job",
            "--run-id",
            "cli-test",
            "--trigger",
            "manual:",
            "--entry-point",
            entry_point,
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0
    assert "backfill_done" in result.stdout


def test_module_launcher_via_cli() -> None:
    """Module launcher works via python -m with CLI args, execvp replaces process."""
    entry_point = json.dumps(
        {
            "module": f"{WORKSPACE}.hello_module",
            "function": None,
            "job_type": "batch",
        }
    )
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "dlt._workspace.deployment.launchers.module",
            "--run-id",
            "mod-cli-test",
            "--trigger",
            "manual:",
            "--entry-point",
            entry_point,
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0
    assert "hello_module_ok" in result.stdout


def test_module_launcher_cli_error_exit_code() -> None:
    """Module launcher returns non-zero exit code for missing module."""
    entry_point = json.dumps(
        {
            "module": "nonexistent.module",
            "function": None,
            "job_type": "batch",
        }
    )
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "dlt._workspace.deployment.launchers.module",
            "--run-id",
            "mod-fail-test",
            "--trigger",
            "manual:",
            "--entry-point",
            entry_point,
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode != 0


def test_job_launcher_cli_error_exit_code() -> None:
    """Job launcher returns non-zero exit code on error."""
    entry_point = json.dumps(
        {
            "module": "nonexistent.module",
            "function": "foo",
            "job_type": "batch",
        }
    )
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "dlt._workspace.deployment.launchers.job",
            "--run-id",
            "fail-test",
            "--trigger",
            "manual:",
            "--entry-point",
            entry_point,
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode != 0
    assert "error" in result.stderr.lower()
