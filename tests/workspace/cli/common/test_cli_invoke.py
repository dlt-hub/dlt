import os
import sys
import pytest
from typing import Any, List, Tuple
from pytest_console_scripts import ScriptRunner
from unittest.mock import patch

import dlt
from dlt.common.runners.venv import Venv

from dlt._workspace.cli import _debug, echo as fmt
from dlt._workspace.cli._dlt import _create_parser, main
from dlt._workspace.cli.exceptions import CliCommandException

BASE_COMMANDS = ["init", "deploy", "pipeline", "telemetry", "schema"]


def _parse_dlt(argv: List[str]) -> Tuple[Any, Any]:
    """Run the dual-parse for the `dlt` host. Returns (installed, args)."""
    parser, pre_parser, installed = _create_parser("dlt")
    ns, remaining = pre_parser.parse_known_args(argv)
    return installed, parser.parse_args(remaining, namespace=ns)


def test_invoke_basic(script_runner: ScriptRunner) -> None:
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
    assert fmt.cli_cmd("pipeline info") == "dlthub pipeline info"
    assert fmt.cli_cmd() == "dlthub"

    fmt.set_cli_host_name("dlt")
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
    script_runner: ScriptRunner, flags: list[str], confirms: bool
) -> None:
    """Destructive commands like sync and drop ask for confirmation with default=False.
    --non-interactive uses the default, so nothing happens. -y/--yes overrides to True, so the commands actually execute.
    """
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
