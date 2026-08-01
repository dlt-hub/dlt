import os
import sys
import pytest
from typing import Any, List, Tuple
from pytest_console_scripts import ScriptRunner
from unittest.mock import MagicMock, patch

import dlt
from dlt.version import __version__
from dlt.common.runners.venv import Venv
from dlt.common import known_env

from dlt._workspace.cli import _debug, echo as fmt
from dlt._workspace.cli._dlt import _create_parser, _main, main
from dlt._workspace.cli.exceptions import CliCommandException

from tests.workspace.utils import (
    fruitshop_pipeline_context as fruitshop_pipeline_context,
    isolated_workspace,
)

BASE_COMMANDS = ["init", "deploy", "pipeline", "telemetry", "schema"]


def _parse_dlt(argv: List[str]) -> Tuple[Any, Any]:
    """Run the dual-parse for the `dlt` host. Returns (installed, args)."""
    parser, pre_parser, installed = _create_parser("dlt")
    ns, remaining = pre_parser.parse_known_args(argv)
    return installed, parser.parse_args(remaining, namespace=ns)


def test_invoke_basic(legacy_workspace_context, script_runner: ScriptRunner) -> None:
    # legacy_workspace_context nests a non-workspace `RunContext` inside the autouse
    # workspace so `_main()` keeps the `dlt` host (no workspace handoff) and exercises
    # the `dlt` host commands directly.
    result = script_runner.run(["dlt", "--version"])
    assert result.returncode == 0
    assert result.stdout.startswith("dlt ")
    assert result.stderr == ""

    result = script_runner.run(["dlt", "--version"], shell=True)
    assert result.returncode == 0
    assert result.stdout.startswith("dlt ")
    assert result.stderr == ""

    for command in BASE_COMMANDS:
        result = script_runner.run(["dlt", command, "--help"])
        assert result.returncode == 0
        assert result.stdout.startswith(f"Usage: dlt {command}")

    result = script_runner.run(["dlt", "N/A", "--help"])
    assert result.returncode != 0


@pytest.mark.parametrize("host", ["dlt", "dlthub"], ids=["dlt", "dlthub"])
def test_parser_prog_matches_host(host: str) -> None:
    parser, _pre, _installed = _create_parser(host)
    assert parser.prog == host


def test_main_sets_active_host(monkeypatch: pytest.MonkeyPatch) -> None:
    # invoke `dlt --version` so main returns without dispatching a subcommand
    monkeypatch.setattr("sys.argv", ["dlt"])
    main("dlthub")
    assert fmt.get_cli_host_name() == "dlthub"


def test_cli_cmd_formats_with_active_host() -> None:
    fmt.set_cli_host_name("dlthub")
    # pipeline commands are mapped to the verb-first `local pipeline` form
    assert fmt.cli_cmd("pipeline info") == "dlthub local pipeline info"
    assert fmt.cli_cmd("pipeline my_pipe trace") == "dlthub local pipeline trace my_pipe"
    assert (
        fmt.cli_cmd("pipeline my_pipe load-package 123 row-counts")
        == "dlthub local pipeline load-package my_pipe 123 row-counts"
    )
    assert fmt.cli_cmd("pipeline -v my_pipe info") == "dlthub local pipeline -v info my_pipe"
    assert fmt.cli_cmd("pipeline my_pipe sync") == "dlthub local pipeline sync my_pipe"
    # non-pipeline commands only swap the group word
    assert fmt.cli_cmd("schema") == "dlthub local schema"
    assert fmt.cli_cmd("init pokemon bigquery") == "dlthub pipeline init pokemon bigquery"
    assert fmt.cli_cmd() == "dlthub"

    fmt.set_cli_host_name("dlt")
    # dlt host keeps the canonical OSS form
    assert (
        fmt.cli_cmd("pipeline my_pipe load-package 123 row-counts")
        == "dlt pipeline my_pipe load-package 123 row-counts"
    )
    assert fmt.cli_cmd("init") == "dlt init"


def test_help_text_uses_active_host() -> None:
    parser, _pre, _installed = _create_parser("dlthub")
    help_text = parser.format_help()
    assert "dlthub" in help_text
    # usage line opens with `usage: dlthub` (rich_argparse capitalises to `Usage:`)
    assert help_text.lower().startswith("usage: dlthub")


def test_create_parser_filters_none_hookimpls() -> None:
    """Built-in workspace+profile hookimpls return None when workspace inactive — must not crash."""
    # create_parser already handles this in non-workspace context. Calling it here in
    # the isolated workspace should still produce a populated subcommand list without
    # raising on the None values that other plugins may yield for unknown hosts.
    parser, _pre, installed = _create_parser("dlt")
    # init must be present regardless of workspace state
    assert "init" in installed
    # parser was built without raising
    assert parser is not None


@pytest.mark.parametrize(
    "argv,expected_verbosity",
    [
        (["pipeline", "-v"], 1),
        (["-v", "pipeline"], 1),
        (["-v", "pipeline", "-v"], 2),
        (["pipeline", "-vv"], 2),
        (["pipeline", "-vvv"], 3),
        (["pipeline"], 0),
    ],
    ids=["after-cmd", "before-cmd", "interleaved-2x", "vv-token", "vvv-token", "no-flag"],
)
def test_verbose_at_any_position(argv: List[str], expected_verbosity: int) -> None:
    _, args = _parse_dlt(argv)
    assert args.verbosity == expected_verbosity


def test_debug_after_subcommand() -> None:
    assert not _debug.is_debug_enabled()
    _parse_dlt(["pipeline", "--debug"])
    assert _debug.is_debug_enabled()


def test_yes_after_subcommand() -> None:
    assert fmt.ALWAYS_CONFIRM is False
    _parse_dlt(["pipeline", "-y"])
    assert fmt.ALWAYS_CONFIRM is True


def test_yes_long_after_subcommand() -> None:
    _parse_dlt(["pipeline", "--yes"])
    assert fmt.ALWAYS_CONFIRM is True


def test_non_interactive_after_subcommand() -> None:
    assert fmt.ALWAYS_CHOOSE_DEFAULT is False
    _parse_dlt(["pipeline", "--non-interactive"])
    assert fmt.ALWAYS_CHOOSE_DEFAULT is True


@pytest.mark.parametrize(
    "argv",
    [
        ["pipeline", "--enable-telemetry"],
        ["pipeline", "--disable-telemetry"],
        ["pipeline", "--no-pwd"],
        ["pipeline", "--version"],
    ],
    ids=["enable-telemetry", "disable-telemetry", "no-pwd", "version"],
)
def test_top_only_flags_after_subcommand_error(argv: List[str]) -> None:
    """Flags not in the anywhere-globals set must not be accepted post-subcommand."""
    with pytest.raises(SystemExit):
        _parse_dlt(argv)


@pytest.mark.parametrize(
    "flags,confirms",
    [
        (["-y"], True),
        (["--yes"], True),
        (["--yes", "--non-interactive"], True),
        (["-y", "--non-interactive"], True),
        (["--non-interactive"], False),
    ],
    ids=[
        "short",
        "long",
        "yes-and-non-interactive",
        "y-and-non-interactive",
        "non-interactive-only",
    ],
)
def test_yes_flag_auto_confirms(
    legacy_workspace_context,
    script_runner: ScriptRunner,
    flags: list[str],
    confirms: bool,
) -> None:
    """Destructive commands like sync and drop ask for confirmation with default=False.
    --non-interactive uses the default, so nothing happens. -y/--yes overrides to True, so the commands actually execute.
    """
    # legacy_workspace_context activates a legacy RunContext (keeping the full `dlt` host
    # command set instead of the slim `dlthub` set) and pins DLT_DATA_DIR so subprocesses
    # share the parent's pipeline state location.
    result = script_runner.run(["dlt", "init", "chess", "duckdb"])
    assert result.returncode == 0

    os.environ.pop("DESTINATION__DUCKDB__CREDENTIALS", None)
    venv = Venv.restore_current()
    venv.run_script("chess_pipeline.py")

    # sync
    result = script_runner.run(["dlt", *flags, "pipeline", "chess_pipeline", "sync"])
    assert result.returncode == 0, f"STDERR: {result.stderr}"
    if confirms:
        assert "Dropping local state" in result.stdout
        assert "Restoring from destination" in result.stdout
    else:
        assert "Dropping local state" not in result.stdout

    pipeline = dlt.attach(pipeline_name="chess_pipeline")
    assert "players_games" in pipeline.default_schema.tables

    # drop
    result = script_runner.run(
        ["dlt", *flags, "pipeline", "chess_pipeline", "drop", "players_games"]
    )
    assert result.returncode == 0, f"STDERR: {result.stderr}"
    pipeline = dlt.attach(pipeline_name="chess_pipeline")
    if confirms:
        assert "Selected resource(s): ['players_games']" in result.stdout
        assert "players_games" not in pipeline.default_schema.tables
    else:
        assert "players_games" in pipeline.default_schema.tables


@pytest.mark.skipif(sys.stdin.isatty(), reason="stdin connected, test skipped")
def test_no_tty() -> None:
    with fmt.maybe_no_stdin():
        assert fmt.confirm("test", default=True) is True
        assert fmt.prompt("test prompt", ("y", "n"), default="y") == "y"


def test_is_interactive_default_state() -> None:
    """Default fixture state: no flags, predicate is True."""
    assert fmt.is_interactive() is True


@pytest.mark.parametrize(
    "flag",
    ["non-interactive", "yes", "value-injected"],
)
def test_is_interactive_false_after_flag(flag: str) -> None:
    if flag == "non-interactive":
        fmt.set_non_interactive(True)
    elif flag == "yes":
        fmt.set_auto_yes(True)
    else:
        fmt.ALWAYS_CHOOSE_VALUE = "Y"
    assert fmt.is_interactive() is False


def test_is_interactive_inside_maybe_no_stdin_no_tty() -> None:
    """`maybe_no_stdin()` flips to non-interactive when stdin is not a tty."""
    with patch("sys.stdin") as stdin:
        stdin.isatty.return_value = False
        with fmt.maybe_no_stdin():
            assert fmt.is_interactive() is False
    # restored after context
    assert fmt.is_interactive() is True


def test_yes_implies_non_interactive_for_text_input() -> None:
    """`-y` is non-interactive: text_input falls back to default."""
    fmt.set_auto_yes(True)
    assert fmt.text_input("name?", default="alice") == "alice"


def test_yes_implies_non_interactive_for_prompt() -> None:
    """`-y` is non-interactive: prompt falls back to default."""
    fmt.set_auto_yes(True)
    assert fmt.prompt("pick", choices=("a", "b"), default="a") == "a"


def test_yes_confirm_returns_true() -> None:
    """`-y` short-circuits confirm to True regardless of default."""
    fmt.set_auto_yes(True)
    assert fmt.confirm("ok?", default=False) is True


def test_text_input_no_default_non_interactive_raises() -> None:
    """No default + non-interactive → CliCommandException (not NotImplementedError)."""
    fmt.set_non_interactive(True)
    with pytest.raises(CliCommandException):
        fmt.text_input("name?")


def test_prompt_no_default_under_yes_raises() -> None:
    """`-y` cannot answer a free-form prompt with no default."""
    fmt.set_auto_yes(True)
    with pytest.raises(CliCommandException):
        fmt.prompt("pick", choices=("a", "b"))


def test_confirm_no_default_under_non_interactive_raises() -> None:
    """No default + --non-interactive → CliCommandException (no AssertionError)."""
    fmt.set_non_interactive(True)
    with pytest.raises(CliCommandException):
        fmt.confirm("ok?")


DLTHUB_WORKSPACE_ONLY = {"pipeline", "local", "profile"}
DLTHUB_UNCONDITIONAL = {"init", "ai"}


def test_dlt_host_default_commands_registered() -> None:
    """`dlt` host registers its base set unconditionally — independent of hub state."""
    _, _, installed = _create_parser("dlt")
    expected = set(BASE_COMMANDS) | {"dashboard"}
    assert expected <= set(installed), f"missing dlt host commands: {expected - set(installed)}"


def test_dlthub_in_workspace_registers_full_command_set() -> None:
    """In a workspace, `dlthub` exposes init + ai + workspace-only commands."""
    # autouse `auto_isolated_workspace` already activates a WorkspaceRunContext
    _, _, installed = _create_parser("dlthub")
    expected = DLTHUB_UNCONDITIONAL | DLTHUB_WORKSPACE_ONLY
    assert expected <= set(
        installed
    ), f"missing dlthub workspace commands: {expected - set(installed)}"


def test_dlthub_outside_workspace_registers_slim_command_set(legacy_workspace_context) -> None:
    """Outside a workspace, `dlthub` only exposes init + ai (workspace ones return None)."""
    _, _, installed = _create_parser("dlthub")
    assert DLTHUB_UNCONDITIONAL <= set(installed)
    assert not (
        DLTHUB_WORKSPACE_ONLY & set(installed)
    ), f"workspace-only commands leaked into OSS context: {DLTHUB_WORKSPACE_ONLY & set(installed)}"


def test_dlt_ai_moved_to_dlthub_stub(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`dlt ai` is a stub that swallows argv and redirects to `dlthub ai`."""
    # description (visible in --help) and the runtime warning both mention dlthub
    _, _, installed = _create_parser("dlt")
    assert "ai" in installed
    assert "dlthub" in installed["ai"].description
    assert "dlthub ai" in installed["ai"].description

    # arbitrary trailing argv is swallowed via REMAINDER (no argparse error)
    monkeypatch.setattr("sys.argv", ["dlt", "ai", "init", "--agent", "claude"])
    rc = main("dlt")
    captured = capsys.readouterr()
    combined = captured.out + captured.err
    assert "`ai` command moved to dlthub" in combined
    assert "dlthub ai" in combined
    assert rc != 0


@pytest.mark.parametrize(
    "argv,case_id",
    [
        (["dashboard"], "dashboard"),
        (["pipeline", "any_pipeline", "show"], "pipeline_show"),
    ],
    ids=["dashboard", "pipeline_show"],
)
def test_hub_feature_warns_when_not_found(
    argv: List[str],
    case_id: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Without `dlt[hub]`, the gated commands warn and never reach the launcher."""
    monkeypatch.setattr("dlt.hub.__found__", False)
    # patch the source modules — the gated callers do `from <module> import X`
    # at call time, which resolves the (now-patched) attribute
    dashboard_spy = MagicMock(return_value=None)
    monkeypatch.setattr("dlt._workspace.helpers.dashboard.runner.run_dashboard", dashboard_spy)

    monkeypatch.setattr("sys.argv", ["dlt"] + argv)
    rc = main("dlt")
    captured = capsys.readouterr()

    combined = captured.out + captured.err
    assert "Install" in combined and "dlt[hub]" in combined
    assert dashboard_spy.call_count == 0
    # main() returns 0 because the check is a clean early-return, not an error
    assert rc == 0


def test_hub_dashboard_executes_when_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With `dlt[hub]`, `dlt dashboard` reaches `run_dashboard`."""
    monkeypatch.setattr("dlt.hub.__found__", True)
    spy = MagicMock(return_value=None)
    # patch the source — the wrapper does `from dlt._workspace.helpers.dashboard.runner
    # import run_dashboard` at call time
    monkeypatch.setattr("dlt._workspace.helpers.dashboard.runner.run_dashboard", spy)
    monkeypatch.setattr("sys.argv", ["dlt", "dashboard"])
    rc = main("dlt")
    assert rc == 0
    assert spy.call_count == 1


def test_hub_pipeline_show_passes_check_when_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With `dlt[hub]`, `pipeline show` lets control through to `run_dashboard`"""

    class _Sentinel(Exception):
        pass

    monkeypatch.setattr("dlt.hub.__found__", True)

    def _raise(*_a: Any, **_kw: Any) -> None:
        raise _Sentinel()

    monkeypatch.setattr("dlt._workspace.helpers.dashboard.runner.run_dashboard", _raise)
    monkeypatch.setattr("sys.argv", ["dlt", "pipeline", "any_pipeline", "show"])
    rc = main("dlt")
    assert rc == -1


@pytest.mark.parametrize(
    "argv, expected",
    [
        (["dlt", "init", "pokemon", "duckdb"], "dlthub pipeline init"),
        (["dlt", "pipeline", "my_pipe", "trace"], "dlthub local pipeline"),
        (["dlt", "schema", "--format", "json", "schema.yaml"], "dlthub local schema"),
        (["dlt", "telemetry"], "dlthub local telemetry"),
        (["dlt", "dashboard"], "dlthub local show"),
        (["dlt", "ai", "init"], "dlthub ai"),
        (["dlt", "--debug", "pipeline", "my_pipe"], "dlthub local pipeline"),
        (["dlt", "deploy", "pipe.py", "airflow-composer"], "dlthub local --help"),
        (["dlt", "--help"], "dlthub local --help"),
        (["dlt"], "dlthub local --help"),
    ],
    ids=[
        "init",
        "pipeline",
        "schema-with-flag",
        "telemetry",
        "dashboard",
        "ai",
        "pipeline-after-flag",
        "deploy-generic",
        "help-generic",
        "bare-generic",
    ],
)
def test_main_in_workspace_prints_dlthub_replacement(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    argv: List[str],
    expected: str,
) -> None:
    """`_main()` inside a workspace does not execute and shows the `dlthub` replacement prefix."""
    dispatch = MagicMock()
    monkeypatch.setattr("dlt._workspace.cli._dlt.main", dispatch)
    monkeypatch.setattr("sys.argv", argv)
    with pytest.raises(SystemExit) as ei:
        _main()
    assert ei.value.code == -1
    # nothing got executed
    dispatch.assert_not_called()
    captured = capsys.readouterr()
    # note goes to stderr so stdout stays clean for redirection
    assert captured.out == ""
    assert "dltHub Workspace" in captured.err
    assert expected in captured.err
    # user args are never spliced into the suggestion
    assert "my_pipe" not in captured.err
    assert "schema.yaml" not in captured.err


def test_main_in_workspace_version_works(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`dlt --version` inside a workspace executes normally without the migration note."""
    monkeypatch.setattr("sys.argv", ["dlt", "--version"])
    with pytest.raises(SystemExit) as ei:
        _main()
    assert ei.value.code == 0
    captured = capsys.readouterr()
    assert __version__ in captured.out
    assert "dltHub Workspace" not in captured.out + captured.err


def test_main_outside_workspace_no_handoff_note(
    legacy_workspace_context,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """In OSS context (no workspace), `_main()` executes and does NOT emit the dlthub note."""
    monkeypatch.setattr("dlt._workspace.cli._dlt.main", lambda host: 0)
    monkeypatch.setattr("sys.argv", ["dlt", "pipeline", "--list-pipelines"])
    with pytest.raises(SystemExit) as ei:
        _main()
    assert ei.value.code == 0
    captured = capsys.readouterr()
    combined = captured.out + captured.err
    assert "dltHub Workspace" not in combined


def test_main_outside_workspace_help_no_migration_note(
    legacy_workspace_context,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """In OSS context (no workspace), `dlt --help` shows full help without the migration note."""
    monkeypatch.setattr("sys.argv", ["dlt", "--help"])
    with pytest.raises(SystemExit) as ei:
        _main()
    assert ei.value.code == 0
    captured = capsys.readouterr()
    assert "Usage: dlt" in captured.out
    assert "dltHub Workspace" not in captured.out + captured.err
    # moved `ai` command renders the full extras spec in help (rich markup `[hub]` is escaped)
    assert "dlt[hub]" in captured.out


@pytest.mark.parametrize(
    "argv",
    [
        ["dlthub"],
        ["dlthub", "--help"],
        ["dlthub", "login"],
    ],
    ids=["no-subcommand", "help-flag", "invalid-subcommand"],
)
def test_dlthub_help_outside_workspace_prints_hint(
    legacy_workspace_context,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    argv: List[str],
) -> None:
    """Every help-printing path on dlthub host outside a workspace appends the
    workspace hint: bare invocation, `--help`, and unknown subcommand.
    """
    monkeypatch.setattr("sys.argv", argv)
    try:
        main("dlthub")
    except SystemExit:
        pass
    captured = capsys.readouterr()
    combined = captured.out + captured.err
    assert "Not all dlthub commands are visible" in combined


@pytest.mark.parametrize(
    "argv",
    [
        ["dlthub"],
        ["dlthub", "--help"],
        ["dlthub", "nope_xyz"],
    ],
    ids=["no-subcommand", "help-flag", "invalid-subcommand"],
)
def test_dlthub_help_inside_workspace_no_hint(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    argv: List[str],
) -> None:
    """Inside a workspace all dlthub commands are visible — no hint anywhere."""
    monkeypatch.setattr("sys.argv", argv)
    try:
        main("dlthub")
    except SystemExit:
        pass
    captured = capsys.readouterr()
    combined = captured.out + captured.err
    assert "Not all dlthub commands are visible" not in combined
