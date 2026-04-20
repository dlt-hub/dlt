"""Unit tests for dlt._workspace.cli._run_command helpers."""

import argparse
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytest

from dlt._workspace.cli import _run_command as run_cmd_mod
from dlt._workspace.cli import commands as commands_mod
from dlt._workspace.cli._run_command import (
    build_runtime_entry_point,
    collect_candidates,
    load_manifest,
    pick_launcher,
    promote_file_arg,
    resolve_interval,
    resolve_profile,
    resolve_refresh,
)
from dlt._workspace.cli.exceptions import CliCommandInnerException
from dlt._workspace.deployment.launchers import LAUNCHER_JOB, LAUNCHER_MODULE
from dlt._workspace.deployment.launchers import _launcher as launcher_mod
from dlt._workspace.deployment.manifest import expand_triggers
from dlt._workspace.deployment.typing import (
    TEntryPoint,
    TExecuteSpec,
    TIntervalSpec,
    TJobDefinition,
    TJobRef,
    TRefreshPolicy,
    TRequireSpec,
    TTrigger,
)
from dlt._workspace.profile import DEFAULT_PROFILE


NOW = datetime(2026, 4, 19, 12, 0, tzinfo=timezone.utc)


def _job(
    ref: str,
    *,
    triggers: Optional[List[str]] = None,
    default_trigger: Optional[str] = None,
    job_type: str = "batch",
    function: Optional[str] = "main",
    refresh: Optional[TRefreshPolicy] = None,
    require: Optional[TRequireSpec] = None,
    interval: Optional[TIntervalSpec] = None,
    allow_external_schedulers: bool = False,
    launcher: Optional[str] = None,
) -> TJobDefinition:
    entry: TEntryPoint = {
        "module": "my_mod",
        "function": function,
        "job_type": job_type,  # type: ignore[typeddict-item]
        "launcher": launcher or "dlt._workspace.deployment.launchers.job",
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
    if refresh is not None:
        jd["refresh"] = refresh
    if require is not None:
        jd["require"] = require
    if interval is not None:
        jd["interval"] = interval
    if allow_external_schedulers:
        jd["allow_external_schedulers"] = True
    return jd


def _expanded(jobs: List[TJobDefinition]) -> Dict[str, List[TTrigger]]:
    return {j["job_ref"]: expand_triggers(j) for j in jobs}


@pytest.mark.parametrize(
    "positional,file,expected",
    [
        ("batch.run", None, ("batch.run", None)),
        (None, "mod.py", (None, "mod.py")),
        (None, None, (None, None)),
    ],
    ids=["non-py-positional", "file-only", "both-none"],
)
def test_promote_file_arg_passthrough(
    positional: Optional[str],
    file: Optional[str],
    expected: Tuple[Optional[str], Optional[str]],
) -> None:
    assert promote_file_arg(positional, file) == expected


def test_promote_file_arg_promotes_py(tmp_path: Path) -> None:
    py = tmp_path / "jobs.py"
    py.write_text("")
    assert promote_file_arg(str(py), None) == (None, str(py))


def test_promote_file_arg_conflict_raises(tmp_path: Path) -> None:
    py = tmp_path / "a.py"
    py.write_text("")
    with pytest.raises(CliCommandInnerException, match="both"):
        promote_file_arg(str(py), "b.py")


def test_promote_file_arg_missing_file_raises() -> None:
    with pytest.raises(CliCommandInnerException, match="not found"):
        promote_file_arg("does_not_exist.py", None)


@pytest.mark.parametrize(
    "job_kwargs,selector,expected_ref,expected_trigger",
    [
        (
            {"default_trigger": "schedule:0 * * * *"},
            None,
            "jobs.a",
            "schedule:0 * * * *",
        ),
        ({}, "jobs.a", "jobs.a", "manual:jobs.a"),
        (
            {"triggers": ["tag:daily", "manual:jobs.a"]},
            "tag:daily",
            "jobs.a",
            "tag:daily",
        ),
    ],
    ids=[
        "no-selector-uses-default-trigger",
        "bare-ref-resolves-to-manual",
        "tag-selector-pattern",
    ],
)
def test_collect_candidates_picks_expected(
    job_kwargs: Dict[str, Any],
    selector: Optional[str],
    expected_ref: str,
    expected_trigger: str,
) -> None:
    jobs = [_job("jobs.a", **job_kwargs)]
    candidates = collect_candidates(jobs, selector, _expanded(jobs))
    assert len(candidates) == 1
    assert candidates[0][0]["job_ref"] == expected_ref
    assert candidates[0][1] == expected_trigger


def test_collect_candidates_no_selector_synthesizes_manual_for_jobs_without_default() -> None:
    jobs = [
        _job("jobs.a", default_trigger="manual:jobs.a"),
        _job("jobs.b"),  # no default_trigger
    ]
    candidates = collect_candidates(jobs, None, _expanded(jobs))
    assert [j["job_ref"] for j, _ in candidates] == ["jobs.a", "jobs.b"]
    assert candidates[0][1] == "manual:jobs.a"
    assert candidates[1][1] == "manual:jobs.b"


def test_collect_candidates_no_match_returns_empty() -> None:
    jobs = [_job("jobs.a", triggers=["manual:jobs.a"])]
    assert collect_candidates(jobs, "tag:*", _expanded(jobs)) == []


def test_collect_candidates_invalid_ref_raises() -> None:
    jobs = [_job("jobs.a")]
    with pytest.raises(CliCommandInnerException, match="Could not resolve"):
        collect_candidates(jobs, "does_not_exist", _expanded(jobs))


@pytest.mark.parametrize(
    "policy,user_flag,expected_refresh,expect_warning",
    [
        ("auto", False, False, False),
        ("auto", True, True, False),
        ("always", False, True, False),
        ("always", True, True, False),
        ("block", False, False, False),
        ("block", True, False, True),
        (None, True, True, False),
    ],
    ids=[
        "auto-off",
        "auto-on",
        "always-off-forces-refresh",
        "always-on-forces-refresh",
        "block-off",
        "block-on-ignored",
        "default-is-auto",
    ],
)
def test_resolve_refresh(
    policy: Optional[TRefreshPolicy],
    user_flag: bool,
    expected_refresh: bool,
    expect_warning: bool,
) -> None:
    jd = _job("jobs.a", refresh=policy)
    effective, warning = resolve_refresh(user_flag, jd)
    assert effective is expected_refresh
    if expect_warning:
        assert warning and "refresh=block" in warning
    else:
        assert warning is None


@pytest.mark.parametrize(
    "user,declared,pinned,expected_current,expect_warning",
    [
        ("access", None, "dev", "access", False),
        (None, None, "tests", "tests", False),
        (None, None, None, DEFAULT_PROFILE, False),
        (None, "prod", "prod", "prod", False),
        (None, "prod", "dev", "dev", True),
        ("dev", "prod", "prod", "dev", True),
    ],
    ids=[
        "cli-override-wins",
        "pinned-when-no-override",
        "fallback-to-default",
        "no-warning-when-declared-matches",
        "warns-on-declared-vs-pinned-mismatch",
        "cli-override-triggers-declared-mismatch",
    ],
)
def test_resolve_profile(
    user: Optional[str],
    declared: Optional[str],
    pinned: Optional[str],
    expected_current: str,
    expect_warning: bool,
) -> None:
    require: Optional[TRequireSpec] = {"profile": declared} if declared else None
    jd = _job("jobs.a", require=require)
    current, warning = resolve_profile(user, jd, pinned)
    assert current == expected_current
    if expect_warning:
        assert warning and f"'{declared}'" in warning and f"'{expected_current}'" in warning
    else:
        assert warning is None


@pytest.mark.parametrize(
    "user_start,user_end,job_kwargs,expected_start,expected_end,expected_tz",
    [
        (
            "2026-04-19T00:00:00Z",
            "2026-04-19T06:00:00Z",
            {},
            datetime(2026, 4, 19, 0, tzinfo=timezone.utc),
            datetime(2026, 4, 19, 6, tzinfo=timezone.utc),
            "UTC",
        ),
        (
            "2026-04-19T00:00:00Z",
            None,
            {},
            datetime(2026, 4, 19, 0, tzinfo=timezone.utc),
            NOW,
            "UTC",
        ),
        (
            "2026-04-19T12:00:00",
            "2026-04-19T13:00:00",
            {"require": {"timezone": "Europe/Warsaw"}},
            datetime(2026, 4, 19, 10, tzinfo=timezone.utc),
            datetime(2026, 4, 19, 11, tzinfo=timezone.utc),
            "Europe/Warsaw",
        ),
        (
            None,
            None,
            {
                "triggers": ["schedule:0 * * * *"],
                "default_trigger": "schedule:0 * * * *",
                "interval": {
                    "start": "2026-04-19T00:00:00Z",
                    "end": "2026-04-19T06:00:00Z",
                },
            },
            datetime(2026, 4, 19, 0, tzinfo=timezone.utc),
            datetime(2026, 4, 19, 6, tzinfo=timezone.utc),
            "UTC",
        ),
        (
            None,
            None,
            {
                "triggers": ["schedule:0 * * * *"],
                "default_trigger": "schedule:0 * * * *",
            },
            datetime(2026, 4, 19, 11, tzinfo=timezone.utc),
            datetime(2026, 4, 19, 12, tzinfo=timezone.utc),
            "UTC",
        ),
        (None, None, {}, NOW, NOW, "UTC"),
    ],
    ids=[
        "user-override-full",
        "user-start-only-end-defaults-to-now",
        "naive-values-use-job-tz-Warsaw-CEST",
        "job-def-interval-with-cron",
        "fallback-compute-run-interval-schedule",
        "manual-collapses-to-point",
    ],
)
def test_resolve_interval(
    user_start: Optional[str],
    user_end: Optional[str],
    job_kwargs: Dict[str, Any],
    expected_start: datetime,
    expected_end: datetime,
    expected_tz: str,
) -> None:
    jd = _job("jobs.a", **job_kwargs)
    start, end, tz = resolve_interval(user_start, user_end, jd, TTrigger("manual:jobs.a"), NOW)
    assert start == expected_start
    assert end == expected_end
    assert tz == expected_tz


def test_build_runtime_entry_point_batch_sets_interval_and_profile() -> None:
    jd = _job("jobs.a")
    start = datetime(2026, 4, 19, 10, tzinfo=timezone.utc)
    end = datetime(2026, 4, 19, 11, tzinfo=timezone.utc)
    ep = build_runtime_entry_point(
        jd,
        {},
        profile="prod",
        refresh=True,
        interval_start=start,
        interval_end=end,
        tz="UTC",
    )
    assert ep["interval_start"] == "2026-04-19T10:00:00+00:00"
    assert ep["interval_end"] == "2026-04-19T11:00:00+00:00"
    assert ep["interval_timezone"] == "UTC"
    assert ep["profile"] == "prod"
    assert ep["refresh"] is True
    assert ep["allow_external_schedulers"] is False
    assert "run_args" not in ep


def test_build_runtime_entry_point_interactive_sets_port() -> None:
    jd = _job("jobs.dash", job_type="interactive")
    ep = build_runtime_entry_point(jd, {}, "dev", False, NOW, NOW, "UTC")
    assert ep["run_args"] == {"port": 5000}


def test_build_runtime_entry_point_config_merges() -> None:
    jd = _job("jobs.a")
    jd["entry_point"]["config"] = {"A": "1", "B": "2"}  # type: ignore[typeddict-unknown-key]
    ep = build_runtime_entry_point(jd, {"B": "override", "C": "3"}, "dev", False, NOW, NOW, "UTC")
    assert ep["config"] == {"A": "1", "B": "override", "C": "3"}


def test_build_runtime_entry_point_propagates_allow_external_schedulers() -> None:
    jd = _job("jobs.a", allow_external_schedulers=True)
    ep = build_runtime_entry_point(jd, {}, "dev", False, NOW, NOW, "UTC")
    assert ep["allow_external_schedulers"] is True


def test_build_runtime_entry_point_does_not_mutate_job_def() -> None:
    jd = _job("jobs.a")
    original_entry = dict(jd["entry_point"])
    build_runtime_entry_point(jd, {"X": "1"}, "prod", True, NOW, NOW, "UTC")
    assert dict(jd["entry_point"]) == original_entry


@pytest.mark.parametrize(
    "launcher,function,expected",
    [
        ("custom.launcher", "main", "custom.launcher"),
        (None, "main", LAUNCHER_JOB),
        (None, None, LAUNCHER_MODULE),
    ],
    ids=["explicit-override", "function-based", "module-level"],
)
def test_pick_launcher(launcher: Optional[str], function: Optional[str], expected: str) -> None:
    ep = {"launcher": launcher, "function": function, "job_type": "batch"}
    assert pick_launcher(ep) == expected  # type: ignore[arg-type]


def test_load_manifest_plain_python_module_becomes_module_job(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    script = tmp_path / "my_pipeline.py"
    script.write_text("x = 1\n")
    monkeypatch.chdir(tmp_path)
    manifest, _ = load_manifest("my_pipeline.py", use_all=False)
    jobs = manifest["jobs"]
    assert len(jobs) == 1
    assert jobs[0]["entry_point"]["launcher"] == LAUNCHER_MODULE
    assert jobs[0]["entry_point"]["function"] is None


def test_load_manifest_missing_default_module_explains_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    with pytest.raises(CliCommandInnerException, match="__deployment__"):
        load_manifest("__deployment__", use_all=True)


def test_load_manifest_missing_file_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    with pytest.raises(CliCommandInnerException, match="Could not import"):
        load_manifest("does_not_exist_abc123", use_all=True)


def test_load_manifest_import_error_inside_file_surfaces_real_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    script = tmp_path / "bad.py"
    script.write_text("import definitely_not_installed_xyz123\n")
    monkeypatch.chdir(tmp_path)
    with pytest.raises(CliCommandInnerException, match="Failed to import 'bad.py'"):
        load_manifest("bad.py", use_all=False)


def _invoke_workspace_run(monkeypatch: pytest.MonkeyPatch, *cli_args: str) -> Tuple[int, str, str]:
    """Run `workspace run` in-process; intercept exec_process to run the child
    synchronously into explicit buffers and return (returncode, stdout, stderr).
    """
    stdout_buf: List[str] = []
    stderr_buf: List[str] = []

    def _sync_exec(argv: List[str]) -> None:
        result = subprocess.run(argv, capture_output=True, text=True, timeout=60)
        stdout_buf.append(result.stdout)
        stderr_buf.append(result.stderr)
        raise SystemExit(result.returncode)

    monkeypatch.setattr(launcher_mod, "exec_process", _sync_exec)

    cmd = commands_mod.WorkspaceCommand()  # type: ignore[abstract]
    parser = argparse.ArgumentParser(prog="dlt workspace")
    cmd.configure_parser(parser)
    args = parser.parse_args(["run", *cli_args])

    returncode = 0
    try:
        cmd._execute_run(args)
    except SystemExit as exc:
        returncode = int(exc.code or 0)

    return returncode, "".join(stdout_buf), "".join(stderr_buf)


def test_workspace_run_plain_module_end_to_end(
    auto_isolated_workspace: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ws_dir = auto_isolated_workspace.run_dir
    # work is under __main__ guard so load_manifest's import is a no-op and
    # only the launcher subprocess produces output
    (Path(ws_dir) / "hello.py").write_text(
        "if __name__ == '__main__':\n    print('greetings from pipeline')\n"
    )
    returncode, stdout, _ = _invoke_workspace_run(monkeypatch, "hello.py")
    assert returncode == 0
    assert "greetings from pipeline" in stdout


def test_workspace_run_propagates_nonzero_exit(
    auto_isolated_workspace: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ws_dir = auto_isolated_workspace.run_dir
    (Path(ws_dir) / "failing.py").write_text(
        "import sys\n"
        "if __name__ == '__main__':\n"
        "    print('before exit', flush=True)\n"
        "    print('err line', file=sys.stderr, flush=True)\n"
        "    sys.exit(7)\n"
    )
    returncode, stdout, stderr = _invoke_workspace_run(monkeypatch, "failing.py")
    assert returncode == 7
    assert "before exit" in stdout
    assert "err line" in stderr


def test_workspace_run_dry_run_does_not_spawn_subprocess(
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

    cmd = commands_mod.WorkspaceCommand()  # type: ignore[abstract]
    parser = argparse.ArgumentParser(prog="dlt workspace")
    cmd.configure_parser(parser)
    args = parser.parse_args(["run", "dry.py", "--dry-run"])
    cmd._execute_run(args)
    assert "argv" not in called
    assert "dry-run: not launching" in capsys.readouterr().out
