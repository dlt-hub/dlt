"""CLI integration tests for `dlthub local run` / `local serve` / `local pipeline run`."""

import argparse
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytest

from dlt._workspace.cli.dlthub.commands import LocalWorkspaceCommand
from dlt._workspace.deployment import _run_helpers as run_helpers_mod
from dlt._workspace.deployment._run_helpers import fetch_run_info
from dlt._workspace.deployment.exceptions import (
    AmbiguousJobSelector,
    DeploymentException,
    JobRefNotInCandidates,
)
from dlt._workspace.deployment.launchers import _launcher as launcher_mod
from dlt._workspace.deployment.typing import (
    TEntryPoint,
    TExecuteSpec,
    TJobDefinition,
    TJobRef,
    TJobsDeploymentManifest,
    TTrigger,
)


NOW = datetime(2026, 4, 19, 12, 0, tzinfo=timezone.utc)


def _job(
    ref: str,
    *,
    triggers: Optional[List[str]] = None,
    default_trigger: Optional[str] = None,
    job_type: str = "batch",
    function: Optional[str] = "main",
    deliver: Optional[Dict[str, Any]] = None,
) -> TJobDefinition:
    entry: TEntryPoint = {
        "module": "my_mod",
        "function": function,
        "job_type": job_type,  # type: ignore[typeddict-item]
        "launcher": "dlt._workspace.deployment.launchers.job",
    }
    if triggers is None:
        triggers = [f"manual:{ref}"]
    jd: TJobDefinition = {
        "job_ref": TJobRef(ref),
        "entry_point": entry,
        "triggers": [TTrigger(t) for t in triggers],
        "execute": TExecuteSpec(),
    }
    if default_trigger is not None:
        jd["default_trigger"] = TTrigger(default_trigger)
    if deliver is not None:
        jd["deliver"] = deliver  # type: ignore[typeddict-item]
    return jd


def _manifest(jobs: List[TJobDefinition]) -> TJobsDeploymentManifest:
    return {"engine_version": 1, "jobs": jobs}  # type: ignore[typeddict-item]


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
        result = subprocess.run(argv, capture_output=True, text=True, timeout=60)
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


def test_local_run_file_no_short_form() -> None:
    """`-f` is no longer a short alias for --deployment (reserved for future --follow)."""
    cmd = LocalWorkspaceCommand()
    parser = argparse.ArgumentParser(prog="dlthub local")
    cmd.configure_parser(parser)
    with pytest.raises(SystemExit):
        parser.parse_args(["run", "-f", "some.py"])
