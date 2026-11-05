"""CLI integration tests for `dlthub local run` / `local serve` / `local pipeline run`."""

import argparse
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import pytest

from dlt._workspace.cli import echo as fmt
from dlt._workspace.cli._dlt import NonInteractiveAction, YesAction
from dlt._workspace.cli.dlthub import _local_workspace_command as local_serve_mod
from dlt._workspace.cli.dlthub._local_workspace_command import open_serve_browser
from dlt._workspace.cli.dlthub.commands import LocalWorkspaceCommand
from dlt._workspace.deployment import _run_helpers as run_helpers_mod
from dlt._workspace.deployment._run_helpers import fetch_run_info
from dlt._workspace.deployment.exceptions import (
    AmbiguousJobSelector,
    DeploymentException,
    JobRefNotInCandidates,
)
from dlt._workspace.deployment.launchers import LAUNCHER_JOB, _launcher as launcher_mod
from dlt._workspace.deployment.typing import (
    TJobDefinition,
    TJobsDeploymentManifest,
)

from tests.workspace.manifest_utils import make_job, make_manifest


NOW = datetime(2026, 4, 19, 12, 0, tzinfo=timezone.utc)


def _job(
    ref: str,
    *,
    triggers: Optional[List[str]] = None,
    job_type: str = "batch",
    function: Optional[str] = "main",
    launcher: Optional[str] = None,
    **fields: Any,
) -> TJobDefinition:
    return make_job(
        ref,
        job_type=job_type,  # type: ignore[arg-type]
        triggers=[f"manual:{ref}"] if triggers is None else triggers,
        module="my_mod",
        function=function,
        launcher=launcher or LAUNCHER_JOB,
        concurrency=None,
        **{k: v for k, v in fields.items() if v is not None},
    )


def _manifest(jobs: List[TJobDefinition]) -> TJobsDeploymentManifest:
    return make_manifest(jobs)


def _patch_load_manifest(
    monkeypatch: pytest.MonkeyPatch, manifest: TJobsDeploymentManifest
) -> None:
    """Replace `load_manifest_with_warnings` with one that returns `manifest`."""
    monkeypatch.setattr(
        run_helpers_mod,
        "load_manifest_with_warnings",
        lambda name_or_path, use_all=True: (manifest, "fake-hash", []),
    )


def test_fetch_run_info_manual_selector_swaps_to_default_trigger(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A manual: selector substitutes with the job's natural schedule trigger."""
    job_def = _job(
        "jobs.a",
        triggers=["schedule:0 * * * *", "manual:jobs.a"],
        default_trigger="schedule:0 * * * *",
    )
    _patch_load_manifest(monkeypatch, _manifest([job_def]))

    info = fetch_run_info(
        selector="manual:jobs.a",
        pick=lambda candidates: candidates[0],
        now_utc=NOW,
    )
    assert info is not None
    assert info["trigger"] == "schedule:0 * * * *"
    assert info["trigger_humanized"] == "schedule: 0 * * * *"
    assert info["entry_point"]["interval_start"] == "2026-04-19T11:00:00+00:00"
    assert info["entry_point"]["interval_end"] == "2026-04-19T12:00:00+00:00"
    assert info["entry_point"]["refresh"] is False


def test_fetch_run_info_forbids_interactive_when_run(monkeypatch: pytest.MonkeyPatch) -> None:
    """`local run` (forbidden=interactive) on an interactive-only manifest raises."""
    _patch_load_manifest(monkeypatch, _manifest([_job("jobs.dash", job_type="interactive")]))
    with pytest.raises(DeploymentException, match="interactive"):
        fetch_run_info(
            selector="jobs.dash",
            forbidden_job_type="interactive",
            now_utc=NOW,
        )


def test_fetch_run_info_forbids_batch_when_serve(monkeypatch: pytest.MonkeyPatch) -> None:
    """`local serve` (forbidden=batch) on a batch-only manifest raises."""
    _patch_load_manifest(monkeypatch, _manifest([_job("jobs.a")]))
    with pytest.raises(DeploymentException, match="batch"):
        fetch_run_info(
            selector="jobs.a",
            forbidden_job_type="batch",
            now_utc=NOW,
        )


def test_fetch_run_info_pipeline_run_resolves_by_pipeline_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`selectors=[pipeline_name:foo]` matches the job whose deliver targets that pipeline."""
    jobs = [
        _job("jobs.a", deliver={"pipeline_name": "foo"}),
        _job("jobs.b", deliver={"pipeline_name": "bar"}),
    ]
    _patch_load_manifest(monkeypatch, _manifest(jobs))
    info = fetch_run_info(selectors=["pipeline_name:foo"], now_utc=NOW)
    assert info is not None
    assert info["job_ref"] == "jobs.a"


def test_fetch_run_info_pipeline_run_ambiguous_without_job_ref_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    jobs = [
        _job("jobs.a", deliver={"pipeline_name": "shared"}),
        _job("jobs.b", deliver={"pipeline_name": "shared"}),
    ]
    _patch_load_manifest(monkeypatch, _manifest(jobs))
    # `pick=None` → no fallback, AmbiguousJobSelector propagates
    with pytest.raises(AmbiguousJobSelector):
        fetch_run_info(selectors=["pipeline_name:shared"], now_utc=NOW)


def test_fetch_run_info_pipeline_run_job_ref_picks_one(monkeypatch: pytest.MonkeyPatch) -> None:
    jobs = [
        _job("jobs.a", deliver={"pipeline_name": "shared"}),
        _job("jobs.b", deliver={"pipeline_name": "shared"}),
    ]
    _patch_load_manifest(monkeypatch, _manifest(jobs))
    info = fetch_run_info(selectors=["pipeline_name:shared"], job_ref="jobs.b", now_utc=NOW)
    assert info is not None
    assert info["job_ref"] == "jobs.b"


def test_fetch_run_info_job_ref_not_in_candidates_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    jobs = [_job("jobs.a"), _job("jobs.b")]
    _patch_load_manifest(monkeypatch, _manifest(jobs))
    with pytest.raises(JobRefNotInCandidates) as exc:
        fetch_run_info(selector="manual:", job_ref="jobs.c", now_utc=NOW)
    assert "jobs.a" in str(exc.value) and "jobs.b" in str(exc.value)


def test_fetch_run_info_job_ref_with_single_match_mismatch_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Selector matches one job, but `--job-ref` points elsewhere → strict error, no fallback."""
    _patch_load_manifest(monkeypatch, _manifest([_job("jobs.a")]))
    with pytest.raises(JobRefNotInCandidates):
        fetch_run_info(selector="jobs.a", job_ref="jobs.b", now_utc=NOW)


def _invoke_local(monkeypatch: pytest.MonkeyPatch, op: str, *cli_args: str) -> Tuple[int, str, str]:
    """Run a `local <op>` end-to-end with a captured-subprocess `exec_process`."""
    stdout_buf: List[str] = []
    stderr_buf: List[str] = []

    def _sync_exec(argv: List[str]) -> None:
        result = subprocess.run(argv, capture_output=True, encoding="utf-8", timeout=60)
        stdout_buf.append(result.stdout)
        stderr_buf.append(result.stderr)
        raise SystemExit(result.returncode)

    monkeypatch.setattr(launcher_mod, "exec_process", _sync_exec)

    cmd = LocalWorkspaceCommand()
    parser = argparse.ArgumentParser(prog="dlthub local")
    cmd.configure_parser(parser)
    args = parser.parse_args([op, *cli_args])

    returncode = 0
    try:
        cmd.execute(args)
    except SystemExit as exc:
        returncode = int(exc.code or 0)

    return returncode, "".join(stdout_buf), "".join(stderr_buf)


def test_local_run_plain_module_end_to_end(
    auto_isolated_workspace: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws_dir = auto_isolated_workspace.run_dir
    (Path(ws_dir) / "hello.py").write_text(
        "if __name__ == '__main__':\n    print('greetings from pipeline')\n"
    )
    returncode, stdout, _ = _invoke_local(monkeypatch, "run", "hello.py")
    assert returncode == 0
    assert "greetings from pipeline" in stdout


def test_local_run_propagates_nonzero_exit(
    auto_isolated_workspace: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws_dir = auto_isolated_workspace.run_dir
    (Path(ws_dir) / "failing.py").write_text(
        "import sys\n"
        "if __name__ == '__main__':\n"
        "    print('before exit', flush=True)\n"
        "    print('err line', file=sys.stderr, flush=True)\n"
        "    sys.exit(7)\n"
    )
    returncode, stdout, stderr = _invoke_local(monkeypatch, "run", "failing.py")
    assert returncode == 7
    assert "before exit" in stdout
    assert "err line" in stderr


def test_local_run_dry_run_does_not_spawn_subprocess(
    auto_isolated_workspace: Any,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    ws_dir = auto_isolated_workspace.run_dir
    (Path(ws_dir) / "dry.py").write_text(
        "if __name__ == '__main__':\n    print('should not run')\n"
    )

    called: Dict[str, Any] = {}

    def _should_not_be_called(argv: List[str]) -> None:
        called["argv"] = argv

    monkeypatch.setattr(launcher_mod, "exec_process", _should_not_be_called)

    cmd = LocalWorkspaceCommand()
    parser = argparse.ArgumentParser(prog="dlthub local")
    cmd.configure_parser(parser)
    args = parser.parse_args(["run", "dry.py", "--dry-run"])
    cmd.execute(args)
    assert "argv" not in called
    assert "dry-run: not launching" in capsys.readouterr().out


def test_local_run_banner_includes_local_chip_and_profile(
    auto_isolated_workspace: Any,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    ws_dir = auto_isolated_workspace.run_dir
    (Path(ws_dir) / "hello.py").write_text("if __name__ == '__main__':\n    print('hi')\n")

    monkeypatch.setattr(launcher_mod, "exec_process", lambda argv: None)

    cmd = LocalWorkspaceCommand()
    parser = argparse.ArgumentParser(prog="dlthub local")
    cmd.configure_parser(parser)
    args = parser.parse_args(["run", "hello.py"])
    cmd.execute(args)

    out = capsys.readouterr().out
    assert "Starting" in out
    assert "[" in out and "local" in out
    assert "job_ref:" in out
    assert "trigger:" in out
    assert "profile:" in out


def test_local_serve_argparse_present() -> None:
    """`local serve` parses with the same positional+job-ref shape as run."""
    cmd = LocalWorkspaceCommand()
    parser = argparse.ArgumentParser(prog="dlthub local")
    cmd.configure_parser(parser)
    args = parser.parse_args(["serve", "jobs.dash", "--job-ref", "jobs.dash"])
    assert args.local_op == "serve"
    assert args.selector_or_job_ref == "jobs.dash"
    assert args.job_ref == "jobs.dash"


def test_local_pipeline_run_argparse_present() -> None:
    """`local pipeline run <name>` parses with --job-ref and --refresh."""
    cmd = LocalWorkspaceCommand()
    parser = argparse.ArgumentParser(prog="dlthub local")
    cmd.configure_parser(parser)
    args = parser.parse_args(["pipeline", "run", "my_pipe", "--job-ref", "jobs.a", "--refresh"])
    assert args.local_op == "pipeline"
    assert args.operation == "run"
    assert args.pipeline_name == "my_pipe"
    assert args.job_ref == "jobs.a"
    assert args.refresh is True


def test_local_pipeline_trace_metrics_argparse() -> None:
    """`local pipeline trace <name> metrics` parses the action and the steps."""
    cmd = LocalWorkspaceCommand()
    parser = argparse.ArgumentParser(prog="dlthub local")
    cmd.configure_parser(parser)
    args = parser.parse_args(
        ["pipeline", "trace", "my_pipe", "metrics", "--steps", "extract", "normalize"]
    )
    assert args.operation == "trace"
    assert args.pipeline_name == "my_pipe"
    assert args.action == "metrics"
    assert args.steps == ["extract", "normalize"]
    # plain trace keeps working without the action
    args = parser.parse_args(["pipeline", "trace", "my_pipe"])
    assert args.action is None
    assert args.steps is None


def test_local_run_file_no_short_form() -> None:
    """`-f` is no longer a short alias for --deployment (reserved for future --follow)."""
    cmd = LocalWorkspaceCommand()
    parser = argparse.ArgumentParser(prog="dlthub local")
    cmd.configure_parser(parser)
    with pytest.raises(SystemExit):
        parser.parse_args(["run", "-f", "some.py"])


def _interactive_job(port: int, ref: str = "jobs.dash") -> TJobDefinition:
    job = _job(ref, job_type="interactive")
    job["entry_point"]["run_args"] = {"port": port}  # type: ignore[typeddict-unknown-key]
    return job


def _install_browser_and_exec(monkeypatch: pytest.MonkeyPatch) -> List[Tuple[str, Any]]:
    """Capture `webbrowser.open` and `exec_process` calls in order, without launching."""
    events: List[Tuple[str, Any]] = []

    def _open(url: str) -> bool:
        events.append(("open", url))
        return True

    def _exec(argv: List[str]) -> None:
        events.append(("exec", argv))

    monkeypatch.setattr(local_serve_mod.webbrowser, "open", _open)
    monkeypatch.setattr(launcher_mod, "exec_process", _exec)
    return events


def _parse_dlthub_local(argv: List[str]) -> argparse.Namespace:
    """Parse global `--non-interactive` / `-y` together with `dlthub local`."""
    parser = argparse.ArgumentParser(prog="dlthub")
    parser.add_argument("--non-interactive", action=NonInteractiveAction)
    parser.add_argument("-y", "--yes", action=YesAction)
    local = parser.add_subparsers(dest="command", required=True).add_parser("local")
    LocalWorkspaceCommand().configure_parser(local)
    return parser.parse_args(argv)


@pytest.mark.parametrize(
    ("host", "url"),
    [
        pytest.param("0.0.0.0", "http://localhost:27182", id="wildcard-ipv4"),
        pytest.param("::", "http://localhost:27182", id="wildcard-ipv6"),
        pytest.param("", "http://localhost:27182", id="empty-host"),
        pytest.param("127.0.0.1", "http://127.0.0.1:27182", id="loopback"),
        pytest.param("localhost", "http://localhost:27182", id="localhost"),
    ],
)
def test_open_serve_browser_uses_listen_host(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    host: str,
    url: str,
) -> None:
    """Wildcard binds open on localhost; any other host is used as given."""
    events = _install_browser_and_exec(monkeypatch)
    open_serve_browser(27182, host)
    assert events == [("open", url)]
    assert "Opening %s" % url in capsys.readouterr().out


def test_local_serve_opens_browser_at_resolved_port(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Interactive serve opens the banner port, after the listen line, before exec."""
    events = _install_browser_and_exec(monkeypatch)
    _patch_load_manifest(monkeypatch, _manifest([_interactive_job(27182)]))
    args = _parse_dlthub_local(["local", "serve", "jobs.dash"])
    LocalWorkspaceCommand().execute(args)

    assert len(events) == 2
    assert events[0] == ("open", "http://localhost:27182")
    assert events[1][0] == "exec"
    out = capsys.readouterr().out
    assert out.index("Listening on http://localhost:27182") < out.index(
        "Opening http://localhost:27182"
    )


def _serve_with_non_interactive_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    args = _parse_dlthub_local(["--non-interactive", "local", "serve", "jobs.dash"])
    LocalWorkspaceCommand().execute(args)


def _serve_with_yes_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    args = _parse_dlthub_local(["--yes", "local", "serve", "jobs.dash"])
    LocalWorkspaceCommand().execute(args)


def _serve_with_non_tty_stdin(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys.stdin, "isatty", lambda: False)
    args = _parse_dlthub_local(["local", "serve", "jobs.dash"])
    with fmt.maybe_no_stdin():
        LocalWorkspaceCommand().execute(args)


@pytest.mark.parametrize(
    "invoke",
    [
        pytest.param(_serve_with_non_interactive_flag, id="flag-non-interactive"),
        pytest.param(_serve_with_yes_flag, id="flag-yes"),
        pytest.param(_serve_with_non_tty_stdin, id="stdin-not-a-tty"),
    ],
)
def test_local_serve_non_interactive_does_not_open_browser(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    invoke: Callable[[pytest.MonkeyPatch], None],
) -> None:
    """A non-interactive serve prints the listen URL and does not open a browser."""
    events = _install_browser_and_exec(monkeypatch)
    _patch_load_manifest(monkeypatch, _manifest([_interactive_job(27182)]))
    invoke(monkeypatch)
    assert len(events) == 1
    assert events[0][0] == "exec"
    out = capsys.readouterr().out
    assert "Opening " not in out
    assert "Listening on http://localhost:27182" in out


def test_local_serve_dry_run_does_not_open_browser(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Dry-run resolves the job and neither launches nor opens a browser."""
    events = _install_browser_and_exec(monkeypatch)
    _patch_load_manifest(monkeypatch, _manifest([_interactive_job(27182)]))
    args = _parse_dlthub_local(["local", "serve", "jobs.dash", "--dry-run"])
    LocalWorkspaceCommand().execute(args)
    assert events == []
    out = capsys.readouterr().out
    assert "--dry-run: not launching" in out
    assert "Opening " not in out
    assert "Listening on " not in out


def test_local_serve_browser_failure_still_serves(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A browser that fails to open does not stop the server from launching."""
    events = _install_browser_and_exec(monkeypatch)

    def _boom(url: str) -> bool:
        raise OSError("no display")

    monkeypatch.setattr(local_serve_mod.webbrowser, "open", _boom)
    _patch_load_manifest(monkeypatch, _manifest([_interactive_job(27182)]))
    args = _parse_dlthub_local(["local", "serve", "jobs.dash"])
    LocalWorkspaceCommand().execute(args)

    assert len(events) == 1
    assert events[0][0] == "exec"
    assert "Opening http://localhost:27182" in capsys.readouterr().out


def test_local_serve_ambiguous_non_interactive_does_not_prompt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two matches and `--non-interactive` raise instead of reading a TTY."""
    events = _install_browser_and_exec(monkeypatch)
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr(sys.stdout, "isatty", lambda: True)
    jobs = [_interactive_job(27182, "jobs.a"), _interactive_job(27183, "jobs.b")]
    _patch_load_manifest(monkeypatch, _manifest(jobs))
    args = _parse_dlthub_local(["--non-interactive", "local", "serve"])
    with pytest.raises(AmbiguousJobSelector):
        LocalWorkspaceCommand().execute(args)
    assert events == []


def test_local_run_does_not_open_browser(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`local run` keeps the banner and does not open a browser."""
    events = _install_browser_and_exec(monkeypatch)
    _patch_load_manifest(monkeypatch, _manifest([_job("jobs.a")]))
    args = _parse_dlthub_local(["local", "run", "jobs.a"])
    LocalWorkspaceCommand().execute(args)
    assert len(events) == 1
    assert events[0][0] == "exec"
    out = capsys.readouterr().out
    assert "Opening " not in out
    assert "Listening on " not in out
