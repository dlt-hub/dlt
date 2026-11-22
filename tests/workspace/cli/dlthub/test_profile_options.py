"""The `--profile`, `--environment` and `--workspace` options of `dlthub` commands."""

import argparse
import os
from types import SimpleNamespace
from typing import Any, List, Optional, Tuple, cast

import pytest

import dlt
from dlt.common.utils import custom_environ

from dlt._workspace._known_env import WORKSPACE__PROFILE
from dlt._workspace._workspace_context import WorkspaceRunContext
from dlt._workspace.cli import SupportsCliCommand
from dlt._workspace.cli._dlt import _create_parser
from dlt._workspace.cli.dlthub._profile_options import (
    PROFILE_OPTIONS_DEST,
    add_profile_options,
)
from dlt._workspace.cli.dlthub.typing import TProfileOptions
from dlt._workspace.environment import ENVIRONMENT_ARGUMENT_HELP, WORKSPACE_ARGUMENT_HELP
from dlt._workspace.exceptions import TProfileSource
from dlt._workspace.profile import PROFILE_ARGUMENT_HELP
from dlt._workspace.settings_files import get_profile_pin_file, save_profile_pin

from tests.workspace.cli.conftest import TMainDlthub, TParseDlthub
from tests.workspace.utils import declare_environment


@pytest.mark.parametrize(
    "argv,expected",
    [
        (["local", "info", "--profile", "prod"], "prod"),
        (["local", "--profile", "prod", "info"], "prod"),
        (["local", "run", "job", "--profile", "prod"], "prod"),
        (["local", "pipeline", "run", "p", "--profile", "prod"], "prod"),
        (["local", "info"], None),
        (["ai", "secrets", "list", "--profile", "prod"], "prod"),
        (["ai", "secrets", "--profile", "prod", "list"], "prod"),
        (["ai", "mcp", "run", "--profile", "prod"], "prod"),
        # bare `ai mcp` runs the server
        (["ai", "mcp", "--profile", "prod"], "prod"),
    ],
    ids=[
        "after-command",
        "at-group",
        "local-run",
        "pipeline-run",
        "absent",
        "ai-secrets",
        "ai-secrets-at-group",
        "ai-mcp-run",
        "ai-mcp",
    ],
)
def test_dlthub_profile_option_position(
    auto_isolated_workspace: WorkspaceRunContext,
    parse_dlthub: TParseDlthub,
    argv: List[str],
    expected: Optional[str],
) -> None:
    args, _ = parse_dlthub(argv)
    assert getattr(args, "profile", None) == expected


@pytest.mark.parametrize(
    "argv",
    [
        ["--profile", "prod", "local", "info"],
        ["--environment", "prod", "local", "info"],
        ["--workspace", "local", "info"],
    ],
    ids=["profile", "environment", "workspace"],
)
def test_dlthub_takes_no_profile_options_before_the_command(
    auto_isolated_workspace: WorkspaceRunContext, main_dlthub: TMainDlthub, argv: List[str]
) -> None:
    assert main_dlthub(*argv)[0] == 2


@pytest.mark.parametrize(
    "argv,remedy",
    [
        (["profile", "list", "--profile", "prod"], "local profile use"),
        (["local", "profile", "use", "prod", "--profile", "prod"], "local profile use"),
        (["init", "--profile", "prod"], "takes no --profile"),
        (["ai", "toolkit", "list", "--profile", "prod"], "takes no --profile"),
        (["ai", "--profile", "prod", "toolkit", "list"], "takes no --profile"),
        (["ai", "status", "--profile", "prod"], "takes no --profile"),
        (["ai", "mcp", "install", "--profile", "prod"], "takes no --profile"),
        (["pipeline", "init", "--profile", "prod"], "takes no --profile"),
        (["local", "schema", "schema.yaml", "--profile", "prod"], "takes no --profile"),
    ],
    ids=[
        "profile",
        "local-profile",
        "init",
        "ai-toolkit",
        "ai-toolkit-at-group",
        "ai-status",
        "ai-mcp-install",
        "pipeline-init",
        "local-schema",
    ],
)
def test_dlthub_profile_commands_take_no_profile_option(
    auto_isolated_workspace: WorkspaceRunContext,
    main_dlthub: TMainDlthub,
    argv: List[str],
    remedy: str,
) -> None:
    code, err = main_dlthub(*argv)
    assert code == 2
    assert remedy in err


@pytest.mark.parametrize(
    "argv",
    [
        ["local", "info"],
        ["local", "profile", "use", "prod"],
        ["profile", "list"],
        ["init"],
        ["pipeline", "init"],
        ["ai", "secrets", "list"],
        ["ai", "mcp", "run"],
    ],
    ids=["local", "local-profile", "profile", "init", "pipeline-init", "ai-secrets", "ai-mcp"],
)
@pytest.mark.parametrize(
    "option,message",
    [
        (["--environment", "prod"], "takes no --environment or --workspace"),
        (["--workspace"], "unrecognized arguments: --workspace"),
    ],
    ids=["environment", "workspace"],
)
def test_dlthub_local_commands_take_no_environment_option(
    auto_isolated_workspace: WorkspaceRunContext,
    main_dlthub: TMainDlthub,
    argv: List[str],
    option: List[str],
    message: str,
) -> None:
    """Only the commands that declare `environment` or `both` take `--environment`, and only the
    ones that also take `--profile` take `--workspace`."""
    code, err = main_dlthub(*argv, *option)
    assert code == 2
    assert message in err


def _get_command_help(parser: argparse.ArgumentParser, *command: str) -> str:
    for name in command:
        subparsers = next(a for a in parser._actions if isinstance(a, argparse._SubParsersAction))
        parser = subparsers.choices[name]
    return parser.format_help()


def _declare(profile_options: TProfileOptions) -> SupportsCliCommand:
    """A command that declares `profile_options`."""
    return cast(SupportsCliCommand, SimpleNamespace(profile_options=profile_options))


def _add_test_commands(parser: argparse.ArgumentParser) -> None:
    """Adds `deploy`, which takes `--environment`, and `variable set`, which takes all profile
    options with the help of `--profile` replaced, as plugins add them."""
    subparsers = next(a for a in parser._actions if isinstance(a, argparse._SubParsersAction))
    deploy_p = subparsers.add_parser("deploy")
    variable_p = subparsers.add_parser("variable")
    variable_p.add_subparsers(dest="op").add_parser("set")
    variable_options: TProfileOptions = {
        "takes": "both",
        "option_help": {"--profile": "Target the scope of this profile"},
    }
    add_profile_options(
        parser,
        [(deploy_p, _declare({"takes": "environment"})), (variable_p, _declare(variable_options))],
    )


@pytest.mark.parametrize(
    "command,shown,hidden",
    [
        ((), [], ["--profile", "--environment", "--workspace"]),
        (("local", "info"), [PROFILE_ARGUMENT_HELP], ["--environment", "--workspace"]),
        (("local", "pipeline"), ["--profile"], ["--environment"]),
        (("local",), [], ["--profile", "--environment"]),
        (("ai", "secrets"), ["--profile"], ["--environment"]),
        (("ai", "mcp", "run"), ["--profile"], ["--environment"]),
        # a group that runs a command of its own shows its options
        (("ai", "mcp"), ["--profile"], ["--environment"]),
        (("ai", "mcp", "install"), [], ["--profile", "--environment"]),
        (("profile", "list"), [], ["--profile", "--environment"]),
        (("deploy",), [ENVIRONMENT_ARGUMENT_HELP], ["--profile", "--workspace"]),
        (
            ("variable",),
            ["Target the scope of this profile", WORKSPACE_ARGUMENT_HELP, "--environment"],
            [],
        ),
        (("variable", "set"), ["Target the scope of this profile", "--workspace"], []),
    ],
    ids=[
        "root",
        "local-info",
        "local-pipeline-group",
        "local-group-of-mixed-commands",
        "ai-secrets-group",
        "ai-mcp-run",
        "ai-mcp-group-that-runs-the-server",
        "ai-mcp-install",
        "profile-list",
        "environment-command",
        "both-group",
        "both-command",
    ],
)
def test_dlthub_help_shows_the_profile_options_of_a_command(
    auto_isolated_workspace: WorkspaceRunContext,
    command: Tuple[str, ...],
    shown: List[str],
    hidden: List[str],
) -> None:
    """A command shows the profile options that it takes, a group the ones that it declares and
    the ones that all of its commands take, with the default help unless the command replaces it."""
    parser, _, _ = _create_parser("dlthub")
    _add_test_commands(parser)
    # rich wraps long help lines
    help_text = " ".join(_get_command_help(parser, *command).split())
    for text in shown:
        assert " ".join(text.split()) in help_text
    for option in hidden:
        assert option not in help_text


def test_dlthub_environment_command(
    auto_isolated_workspace: WorkspaceRunContext,
    monkeypatch: pytest.MonkeyPatch,
    main_dlthub: TMainDlthub,
) -> None:
    """A command that takes `--environment` refuses `--profile` with the remedy."""
    seen: List[argparse.Namespace] = []

    def create_parser(host: str) -> Any:
        parser, pre_parser, installed = _create_parser(host)
        _add_test_commands(parser)
        installed["deploy"] = SimpleNamespace(execute=seen.append, docs_url=None)  # type: ignore[assignment]
        return parser, pre_parser, installed

    monkeypatch.setattr("dlt._workspace.cli._dlt._create_parser", create_parser)
    assert main_dlthub("deploy", "--environment", "prod")[0] == 0
    assert seen[-1].environment == "prod" and getattr(seen[-1], "profile", None) is None
    assert main_dlthub("deploy")[0] == 0
    assert getattr(seen[-1], "environment", None) is None
    # other commands still take --profile
    assert main_dlthub("local", "info", "--profile", "prod")[0] == 0
    code, err = main_dlthub("deploy", "--profile", "prod")
    assert code == 2
    assert "Name the environment with --environment NAME" in err
    # --workspace is --environment without a name and only for commands that take a profile too
    assert "unrecognized arguments: --workspace" in main_dlthub("deploy", "--workspace")[1]


def test_profile_options_declarations() -> None:
    """A declaration applies to the command and the commands under it, `subcommands` replace it
    for the subcommands that they name, and a command attached by another plugin overrides the
    declaration of its parent for itself. A declaration of `subcommands` only keeps the options
    of the command."""
    parser = argparse.ArgumentParser(prog="dlthub")
    jobs = parser.add_subparsers(dest="command").add_parser("job")
    job_ops = jobs.add_subparsers(dest="op")
    job_ops.add_parser("list")
    runs = job_ops.add_parser("runs")
    runs_ops = runs.add_subparsers(dest="runs_op")
    runs_ops.add_parser("list")
    runs_ops.add_parser("cancel")
    attached = job_ops.add_parser("attached")
    job_options: TProfileOptions = {
        "takes": "environment",
        "subcommands": {
            "runs": {"subcommands": {"cancel": {"takes": "both", "required": True}}},
            "attached": {"takes": "none"},
        },
    }
    add_profile_options(
        parser,
        [(jobs, _declare(job_options)), (attached, _declare({"takes": "profile"}))],
    )

    def get_options(*argv: str) -> TProfileOptions:
        options: TProfileOptions = getattr(parser.parse_args(argv), PROFILE_OPTIONS_DEST)
        return options

    assert get_options("job", "list") == {"takes": "environment"}
    assert get_options("job", "runs", "list") == {"takes": "environment"}
    assert get_options("job", "runs", "cancel") == {"takes": "both", "required": True}
    assert get_options("job", "attached") == {"takes": "profile"}

    # a typo in the name of a subcommand shows when the parser is built
    with pytest.raises(ValueError, match="declare the subcommand `cancle`"):
        add_profile_options(parser, [(runs, _declare({"subcommands": {"cancle": {}}}))])


def test_dlthub_environment_command_group(
    auto_isolated_workspace: WorkspaceRunContext,
    monkeypatch: pytest.MonkeyPatch,
    main_dlthub: TMainDlthub,
) -> None:
    """`--environment`, `--workspace` and `--profile` go after the command or its group, also on
    a subcommand that a plugin attaches. `main` runs the command with the profile they select and
    restores the active profile afterwards. A group that takes `--profile` too runs with the
    profile of the environment that it names."""
    seen: List[str] = []

    def execute(args: argparse.Namespace) -> None:
        seen.append(dlt.current.workspace().profile)

    def create_parser(host: str) -> Any:
        parser, pre_parser, installed = _create_parser(host)
        subparsers = next(a for a in parser._actions if isinstance(a, argparse._SubParsersAction))
        remote = subparsers.add_parser("remote")
        remote_ops = remote.add_subparsers(dest="remote_op")
        remote_ops.add_parser("list")
        # as a plugin attaches a sub-subcommand, it declares nothing
        remote_ops.add_parser("late")
        scoped = subparsers.add_parser("scoped")
        scoped_ops = scoped.add_subparsers(dest="scoped_op")
        scoped_ops.add_parser("list")
        scoped_ops.add_parser("set")
        scoped_options: TProfileOptions = {
            "takes": "both",
            "subcommands": {"set": {"takes": "both", "required": True}},
        }
        add_profile_options(
            parser,
            [(remote, _declare({"takes": "environment"})), (scoped, _declare(scoped_options))],
        )
        for name in ("remote", "scoped"):
            installed[name] = SimpleNamespace(execute=execute, docs_url=None)  # type: ignore[assignment]
        return parser, pre_parser, installed

    def run(*argv: str) -> Tuple[int, str]:
        # each run starts with `dev` active, as a new process does
        monkeypatch.delenv(WORKSPACE__PROFILE, raising=False)
        result = main_dlthub(*argv)
        # the command ran in a context of its own
        assert WORKSPACE__PROFILE not in os.environ
        assert dlt.current.workspace().profile == "dev"
        return result

    monkeypatch.setattr("dlt._workspace.cli._dlt._create_parser", create_parser)
    declare_environment(auto_isolated_workspace, "ci")
    for argv, profile in (
        (["remote", "--environment", "ci", "list"], "ci-write"),
        (["remote", "list", "--environment", "ci"], "ci-write"),
        (["remote", "late", "--environment", "ci"], "ci-write"),
        (["remote", "list"], "prod"),
        (["scoped", "list"], "prod"),
        (["scoped", "list", "--workspace"], "prod"),
        (["scoped", "list", "--environment", "ci"], "ci-write"),
        (["scoped", "set", "--workspace"], "prod"),
        # the profile names its environment
        (["scoped", "set", "--profile", "access"], "access"),
        (["scoped", "set", "--profile", "access", "--environment", "prod"], "access"),
        (["scoped", "--profile", "ci-write", "list"], "ci-write"),
        # a group without its subcommand only prints its help
        (["remote"], "dev"),
    ):
        assert run(*argv)[0] == 0, argv
        assert seen[-1] == profile, argv
    for argv, refusal in (
        (["remote", "list", "--profile", "prod"], "--environment"),
        (["remote", "--profile", "prod", "list"], "--environment"),
        (["scoped", "set", "--workspace", "--profile", "access"], "--workspace names no"),
        (["scoped", "set"], "this command needs --profile NAME"),
    ):
        code, err = run(*argv)
        assert code == 2, argv
        assert refusal in err, argv
    code, err = run("scoped", "set", "--profile", "access", "--environment", "ci")
    assert code == -1
    assert "`access` is a profile of environment `prod`, not of `ci`" in err
    code, err = run("scoped", "list", "--profile", "dev")
    assert code == -1
    assert "`dev` is a local profile" in err
    # an active profile of the environment is kept
    with custom_environ({WORKSPACE__PROFILE: "access"}):
        assert main_dlthub("scoped", "list")[0] == 0
        assert seen[-1] == "access"


def test_dlthub_refuses_undeclared_profile(
    auto_isolated_workspace: WorkspaceRunContext,
    monkeypatch: pytest.MonkeyPatch,
    main_dlthub: TMainDlthub,
) -> None:
    """`--profile` takes a declared profile. An environment is not one."""
    monkeypatch.delenv(WORKSPACE__PROFILE, raising=False)
    declare_environment(auto_isolated_workspace, "ci")
    for name, remedy in (
        ("staging", "dlthub profile add staging --local"),
        ("ci", "Name one of its profiles"),
        # a name that cannot be declared gets no remedy that declares it
        ("no/such", "`no/such` is not a valid profile or environment name"),
    ):
        code, err = main_dlthub("local", "info", "--profile", name)
        assert code == -1
        assert remedy in err
        # a refused name does not reach the processes the command starts
        assert WORKSPACE__PROFILE not in os.environ
    assert main_dlthub("local", "info", "--profile", "ci-read")[0] == 0
    assert WORKSPACE__PROFILE not in os.environ


@pytest.mark.parametrize(
    "source", ["recorded", "environ"], ids=["recorded-profile", "environment-variable"]
)
def test_dlthub_stops_on_undeclared_active_profile(
    auto_isolated_workspace: WorkspaceRunContext,
    monkeypatch: pytest.MonkeyPatch,
    main_dlthub: TMainDlthub,
    source: TProfileSource,
) -> None:
    """An undeclared profile recorded in `.dlt/profile-name` or named by `WORKSPACE__PROFILE`
    stops every command when the workspace loads."""
    monkeypatch.delenv(WORKSPACE__PROFILE, raising=False)
    if source == "recorded":
        save_profile_pin(auto_isolated_workspace, "staging")
    else:
        monkeypatch.setenv(WORKSPACE__PROFILE, "staging")
    for argv in (["profile", "list"], ["local", "info"]):
        code, err = main_dlthub(*argv)
        assert code == -1
        assert "`staging` is not a profile declared" in err

    if source == "recorded":
        os.remove(get_profile_pin_file(auto_isolated_workspace))
    else:
        monkeypatch.delenv(WORKSPACE__PROFILE)
    for argv in (["profile", "list"], ["local", "info"]):
        assert main_dlthub(*argv)[0] == 0
