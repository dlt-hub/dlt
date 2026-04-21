"""Tests for job launchers."""

import json
import os
import signal
import subprocess
import sys
from datetime import datetime, timezone  # noqa: I251
from multiprocessing.dummy import DummyProcess
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest

import dlt
from dlt._workspace.cli._run_command import build_runtime_entry_point
from dlt._workspace.deployment.exceptions import JobResolutionError
from dlt._workspace.deployment.launchers.job import run as job_run
from dlt._workspace.deployment.typing import TJobDefinition, TRuntimeEntryPoint
from dlt.common.exceptions import SignalReceivedException
from dlt.common.runtime import signals
from dlt.pipeline.exceptions import PipelineStepFailed

from tests.utils import skipifwindows
from tests.workspace.cases.runtime_workspace import batch_jobs
from tests.workspace.utils import isolated_workspace

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


def test_job_launcher_interval_injection() -> None:
    """Job with interval in entry_point gets it in run_context and dlt.current.interval()."""
    ep = _entry(f"{WORKSPACE}.batch_jobs", "interval_aware")
    ep["interval_start"] = "2024-01-15T00:00:00Z"
    ep["interval_end"] = "2024-01-16T00:00:00Z"
    result = job_run(ep, run_id="iv-test-1", trigger="schedule:0 0 * * *")
    assert "ctx_start=2024-01-15" in result
    assert "current_start=2024-01-15T00:00:00" in result


@pytest.mark.parametrize(
    "iv_tz,expected_iso_fragment",
    [
        ("Europe/Berlin", "2024-01-15T01:00:00+01:00"),  # CET (winter)
        ("America/New_York", "2024-01-14T19:00:00-05:00"),  # EST (winter)
        ("UTC", "2024-01-15T00:00:00+00:00"),
    ],
    ids=["berlin-cet", "new-york-est", "utc"],
)
def test_job_launcher_applies_interval_timezone(iv_tz: str, expected_iso_fragment: str) -> None:
    """Launcher re-applies IANA tz: UTC ISO in entry_point → tz-aware in run_context."""
    ep = _entry(f"{WORKSPACE}.batch_jobs", "interval_aware")
    ep["interval_start"] = "2024-01-15T00:00:00Z"
    ep["interval_end"] = "2024-01-16T00:00:00Z"
    ep["interval_timezone"] = iv_tz
    result = job_run(ep, run_id=f"iv-tz-{iv_tz}", trigger="schedule:0 0 * * *")
    assert f"ctx_start={expected_iso_fragment}" in result
    assert f"current_start={expected_iso_fragment}" in result
    # env vars: UTC ISO + tz name (for subprocesses/direct readers)
    assert os.environ["DLT_INTERVAL_START"] == "2024-01-15T00:00:00Z"
    assert os.environ["DLT_INTERVAL_END"] == "2024-01-16T00:00:00Z"
    assert os.environ["DLT_INTERVAL_TIMEZONE"] == iv_tz


def test_job_launcher_no_interval() -> None:
    """Job without interval in entry_point gets no interval."""
    result = job_run(
        _entry(f"{WORKSPACE}.batch_jobs", "interval_aware"),
        run_id="iv-test-2",
        trigger="manual:",
    )
    assert result == "no_interval"


def test_job_launcher_interval_forces_incremental_join() -> None:
    """Launcher honors `entry_point["allow_external_schedulers"]` so incrementals join."""
    ep = _entry(f"{WORKSPACE}.batch_jobs", "incremental_interval_job")
    ep["interval_start"] = "2024-01-15T00:00:00Z"
    ep["interval_end"] = "2024-01-16T00:00:00Z"
    ep["allow_external_schedulers"] = True
    result = job_run(ep, run_id="inc-iv-1", trigger="schedule:0 0 * * *")
    # incremental joined the scheduler: initial_value and end_value come from the interval
    assert "iv=2024-01-15" in result
    assert "end=2024-01-16" in result
    assert "items=1" in result


def test_job_launcher_interval_without_allow_external_schedulers_does_not_force_join() -> None:
    """Without `allow_external_schedulers`, incrementals do NOT auto-join the runner interval."""
    ep = _entry(f"{WORKSPACE}.batch_jobs", "incremental_interval_job")
    ep["interval_start"] = "2024-01-15T00:00:00Z"
    ep["interval_end"] = "2024-01-16T00:00:00Z"
    # allow_external_schedulers omitted — defaults to False
    result = job_run(ep, run_id="inc-iv-2", trigger="schedule:0 0 * * *")
    # incremental did NOT join: initial_value/end_value are not the interval bounds
    assert "iv=2024-01-15" not in result
    assert "end=2024-01-16" not in result


def test_job_launcher_profile_injection() -> None:
    """Job launcher sets WORKSPACE__PROFILE env var from entry_point.profile."""
    old = os.environ.pop("WORKSPACE__PROFILE", None)
    try:
        ep = _entry(f"{WORKSPACE}.batch_jobs", "profile_aware")
        ep["profile"] = "staging"
        result = job_run(ep, run_id="profile-1", trigger="manual:")
        assert "profile=staging" in result
    finally:
        if old is not None:
            os.environ["WORKSPACE__PROFILE"] = old
        else:
            os.environ.pop("WORKSPACE__PROFILE", None)


def test_job_launcher_no_profile() -> None:
    """Job without profile in entry_point does not set WORKSPACE__PROFILE."""
    old = os.environ.pop("WORKSPACE__PROFILE", None)
    try:
        result = job_run(
            _entry(f"{WORKSPACE}.batch_jobs", "profile_aware"),
            run_id="profile-2",
            trigger="manual:",
        )
        assert "profile=" in result  # empty — no profile set
    finally:
        if old is not None:
            os.environ["WORKSPACE__PROFILE"] = old


def test_job_launcher_with_config() -> None:
    """Job launcher injects config via entry_point.config."""
    ep = _entry(f"{WORKSPACE}.batch_jobs", "maintenance")
    ep["config"] = {"cleanup_days": "30"}
    result = job_run(ep, run_id="test-2", trigger="manual:")
    assert result is None


def test_job_launcher_missing_config_fails() -> None:
    """Job with required config but no value raises ConfigFieldMissingException."""
    from dlt.common.configuration.exceptions import ConfigFieldMissingException

    with pytest.raises(ConfigFieldMissingException):
        job_run(
            _entry(f"{WORKSPACE}.batch_jobs", "maintenance"),
            run_id="test-missing-cfg",
            trigger="manual:",
        )


def test_job_launcher_function_not_found() -> None:
    with pytest.raises(JobResolutionError, match="(?i)cannot resolve"):
        job_run(
            _entry(f"{WORKSPACE}.batch_jobs", "nonexistent"),
            run_id="test-3",
            trigger="manual:",
        )


def test_job_launcher_module_not_found() -> None:
    with pytest.raises((JobResolutionError, ModuleNotFoundError)):
        job_run(
            _entry("nonexistent.module", "foo"),
            run_id="test-4",
            trigger="manual:",
        )


def test_job_launcher_requires_function() -> None:
    with pytest.raises(JobResolutionError, match="function"):
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
    """Module launcher passes [sys.executable, '-m', <module>] to exec_process."""
    with patch("dlt._workspace.deployment.launchers.module.exec_process") as mock_exec:
        from dlt._workspace.deployment.launchers.module import run

        ep = _entry(f"{WORKSPACE}.etl_script")
        run(ep)
        mock_exec.assert_called_once_with([sys.executable, "-m", f"{WORKSPACE}.etl_script"])


def test_marimo_launcher_builds_correct_args() -> None:
    """Marimo launcher passes `marimo -y run` and correct flags to exec_process."""
    entry_point = _entry(f"{WORKSPACE}.marimo_notebook", port=5000)
    with (
        patch("dlt._workspace.deployment.launchers.marimo.exec_process") as mock_exec,
        patch(
            "dlt._workspace.deployment.launchers.marimo.resolve_module_path",
            return_value="/path/to/notebook.py",
        ),
    ):
        from dlt._workspace.deployment.launchers.marimo import run

        run(entry_point)

        cmd = mock_exec.call_args[0][0]
        # -y suppresses the "Are you sure you want to quit?" SIGINT prompt
        assert cmd[:3] == ["marimo", "-y", "run"]
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
        patch("dlt._workspace.deployment.launchers.marimo.exec_process") as mock_exec,
        patch(
            "dlt._workspace.deployment.launchers.marimo.resolve_module_path",
            return_value="/path/to/notebook.py",
        ),
    ):
        os.environ["JOBS__MARIMO_NOTEBOOK__MARIMO__TOKEN"] = "my-secret"
        try:
            from dlt._workspace.deployment.launchers.marimo import run

            run(entry_point)
        finally:
            del os.environ["JOBS__MARIMO_NOTEBOOK__MARIMO__TOKEN"]

        cmd = mock_exec.call_args[0][0]
        assert "--token" in cmd
        assert "--token-password" in cmd
        assert "my-secret" in cmd


def test_marimo_launcher_with_base_path() -> None:
    """Marimo launcher passes --base-url from run_args.base_path."""
    entry_point = _entry(f"{WORKSPACE}.marimo_notebook", port=5000, base_path="/workspace/123/nb")
    with (
        patch("dlt._workspace.deployment.launchers.marimo.exec_process") as mock_exec,
        patch(
            "dlt._workspace.deployment.launchers.marimo.resolve_module_path",
            return_value="/path/to/notebook.py",
        ),
    ):
        from dlt._workspace.deployment.launchers.marimo import run

        run(entry_point)

        cmd = mock_exec.call_args[0][0]
        assert "--base-url" in cmd
        assert "/workspace/123/nb" in cmd


def test_streamlit_launcher_builds_correct_args() -> None:
    """Streamlit launcher passes `streamlit run` and correct flags to exec_process."""
    entry_point = _entry(f"{WORKSPACE}.streamlit_app", port=8501)
    with (
        patch("dlt._workspace.deployment.launchers.streamlit.exec_process") as mock_exec,
        patch(
            "dlt._workspace.deployment.launchers.streamlit.resolve_module_path",
            return_value="/path/to/app.py",
        ),
    ):
        from dlt._workspace.deployment.launchers.streamlit import run

        run(entry_point)

        cmd = mock_exec.call_args[0][0]
        assert cmd[:2] == ["streamlit", "run"]
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

        run(entry_point)

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

            run(entry_point)

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

            run(entry_point)


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


@pytest.fixture(
    params=["launcher_flat", "launcher_package"],
    ids=["flat", "package"],
)
def launcher_workspace(request: pytest.FixtureRequest):
    """Isolated workspace with batch jobs — flat or package layout."""
    with isolated_workspace(request.param) as ctx:
        yield ctx


@pytest.fixture(
    params=[
        [sys.executable],
        ["uv", "run", "python"],
    ],
    ids=["python", "uv-run-python"],
)
def python_cmd(request: pytest.FixtureRequest) -> List[str]:
    return request.param


def _module_prefix(workspace_name: str) -> str:
    """Module prefix for batch_jobs depending on workspace layout."""
    if workspace_name == "launcher_package":
        return "app.batch_jobs"
    return "batch_jobs"


def _hello_module(workspace_name: str) -> str:
    if workspace_name == "launcher_package":
        return "app.hello_module"
    return "hello_module"


def test_isolated_job_launcher_via_cli(launcher_workspace: object, python_cmd: List[str]) -> None:
    """Job launcher runs batch job in isolated workspace."""
    workspace_name = os.path.basename(os.getcwd())
    module = _module_prefix(workspace_name)
    entry_point = json.dumps(
        {
            "module": module,
            "function": "backfill",
            "job_type": "batch",
            "launcher": "dlt._workspace.deployment.launchers.job",
        }
    )
    result = subprocess.run(
        [
            *python_cmd,
            "-m",
            "dlt._workspace.deployment.launchers.job",
            "--run-id",
            "iso-test",
            "--trigger",
            "manual:",
            "--entry-point",
            entry_point,
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, f"stderr: {result.stderr}"
    assert "backfill_done" in result.stdout


def test_isolated_module_launcher_via_cli(
    launcher_workspace: object, python_cmd: List[str]
) -> None:
    """Module launcher runs plain module in isolated workspace."""
    workspace_name = os.path.basename(os.getcwd())
    module = _hello_module(workspace_name)
    entry_point = json.dumps(
        {
            "module": module,
            "function": None,
            "job_type": "batch",
            "launcher": "dlt._workspace.deployment.launchers.module",
        }
    )
    result = subprocess.run(
        [
            *python_cmd,
            "-m",
            "dlt._workspace.deployment.launchers.module",
            "--run-id",
            "iso-mod-test",
            "--trigger",
            "manual:",
            "--entry-point",
            entry_point,
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, f"stderr: {result.stderr}"
    assert "hello_module_ok" in result.stdout


@skipifwindows
@pytest.mark.parametrize("sig", (signal.SIGINT, signal.SIGTERM))
@pytest.mark.forked
def test_job_launcher_signal_graceful_extract(sig: int) -> None:
    """SIGINT/SIGTERM mid-extract raises PipelineStepFailed and persists a trace."""

    def _killer() -> None:
        batch_jobs.EXTRACT_STARTED.wait(timeout=30)
        os.kill(os.getpid(), sig)

    killer = DummyProcess(target=_killer)
    killer.start()

    with pytest.raises(PipelineStepFailed) as exc_info:
        job_run(
            _entry(f"{WORKSPACE}.batch_jobs", "long_extract"),
            run_id="test-sig-extract",
            trigger="manual:",
        )

    assert exc_info.value.step == "extract"
    assert isinstance(exc_info.value.__cause__, SignalReceivedException)

    # trace was saved by @with_runtime_trace finally-block despite the signal
    pipeline = dlt.attach("signal_long_extract")
    trace = pipeline.last_trace
    assert trace is not None
    extract_steps = [s for s in trace.steps if s.step == "extract"]
    assert len(extract_steps) == 1
    assert extract_steps[0].step_exception is not None


def test_build_runtime_entry_point_propagates_execute_intercept_signals() -> None:
    """`execute.intercept_signals` flows through to `entry_point.intercept_signals`."""
    base_ep = {
        "module": "foo",
        "function": "bar",
        "job_type": "batch",
        "launcher": "dlt._workspace.deployment.launchers.job",
    }
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)

    def _job_def(**extra: Any) -> TJobDefinition:
        jd: Dict[str, Any] = {
            "job_ref": "jobs.test",
            "entry_point": dict(base_ep),
            "triggers": [],
            "execute": {},
        }
        jd.update(extra)
        return jd  # type: ignore[return-value,unused-ignore]

    # absent: launcher default (True) applies, flag not copied
    ep_absent = build_runtime_entry_point(_job_def(), {}, "default", False, now, now, "UTC")
    assert "intercept_signals" not in ep_absent

    # explicit False opts out
    ep_off = build_runtime_entry_point(
        _job_def(execute={"intercept_signals": False}), {}, "default", False, now, now, "UTC"
    )
    assert ep_off["intercept_signals"] is False

    # explicit True copied through
    ep_on = build_runtime_entry_point(
        _job_def(execute={"intercept_signals": True}), {}, "default", False, now, now, "UTC"
    )
    assert ep_on["intercept_signals"] is True


@skipifwindows
@pytest.mark.forked
def test_job_launcher_opt_out_of_signal_interception() -> None:
    """`intercept_signals=False` leaves Python's default SIGINT handler in place."""

    def _killer() -> None:
        batch_jobs.JOB_STARTED.wait(timeout=30)
        os.kill(os.getpid(), signal.SIGINT)

    killer = DummyProcess(target=_killer)
    killer.start()

    ep = _entry(f"{WORKSPACE}.batch_jobs", "wait_for_signal")
    ep["intercept_signals"] = False

    # default handler raises plain KeyboardInterrupt, not dlt's SignalReceivedException
    with pytest.raises(KeyboardInterrupt) as exc_info:
        job_run(ep, run_id="test-no-intercept", trigger="manual:")
    assert type(exc_info.value) is KeyboardInterrupt
