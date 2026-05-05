"""Tests for JobProcess."""

import sys
import time

import pytest

from tests.workspace.runner._runner.process import JobProcess


def test_process_runs_and_captures_output() -> None:
    """Process runs a command and captures stdout."""
    proc = JobProcess(
        "jobs.test.echo",
        [sys.executable, "-c", "print('hello'); print('world')"],
    )
    proc.start()
    exit_code = proc.wait()

    assert exit_code == 0
    lines = proc.drain_output()
    stdout_lines = [line for stream, line in lines if stream == 1]
    assert "hello" in stdout_lines
    assert "world" in stdout_lines


def test_process_captures_stderr() -> None:
    """Process captures stderr separately."""
    proc = JobProcess(
        "jobs.test.err",
        [sys.executable, "-c", "import sys; print('err', file=sys.stderr)"],
    )
    proc.start()
    proc.wait()

    lines = proc.drain_output()
    stderr_lines = [line for stream, line in lines if stream == 2]
    assert "err" in stderr_lines


def test_process_exit_code() -> None:
    """Process reports non-zero exit code."""
    proc = JobProcess(
        "jobs.test.fail",
        [sys.executable, "-c", "raise SystemExit(42)"],
    )
    proc.start()
    exit_code = proc.wait()
    assert exit_code == 42


def test_process_terminate() -> None:
    """Process can be terminated with grace period."""
    proc = JobProcess(
        "jobs.test.sleep",
        [sys.executable, "-c", "import time; time.sleep(60)"],
    )
    proc.start()
    time.sleep(0.2)
    assert proc.is_alive()

    proc.terminate(grace_period=1.0)
    assert not proc.is_alive()
    assert proc.poll() is not None


def test_process_short_name() -> None:
    proc = JobProcess("jobs.module.function", [sys.executable, "-c", "pass"])
    assert proc.short_name == "function"

    proc2 = JobProcess("jobs.standalone", [sys.executable, "-c", "pass"])
    assert proc2.short_name == "standalone"
