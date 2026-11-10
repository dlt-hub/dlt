import argparse
import os
from types import SimpleNamespace
from typing import Any, List, Optional, Type

import pytest

from dlt._workspace._known_env import WORKSPACE__PROFILE
from dlt._workspace._workspace_context import WorkspaceRunContext, switch_context
from dlt._workspace.cli._dlt import (
    _add_profile_argument,
    _create_parser,
    add_environment_argument,
)
from dlt._workspace.environment import connect_remote_workspace
from dlt._workspace.cli.dlthub._profile_command import list_profiles, print_profile_info
from dlt._workspace.cli.dlthub.utils import (
    fetch_environments_list,
    fetch_profile_info,
    fetch_profiles_list,
)
from dlt._workspace.exceptions import (
    EnvironmentAlreadyExists,
    EnvironmentNotDeclared,
    ProfileAlreadyExists,
    ProfileException,
    TProfileSource,
)
from dlt._workspace.settings_files import (
    get_profile_pin_file,
    load_yaml_file,
    profile_settings_files,
    save_profile_pin,
)
from dlt._workspace.workspace_definition import (
    LOCAL_PROFILES,
    default_profile,
    load_workspace_definition,
)

from tests.workspace.cli.conftest import TMainDlthub, TParseDlthub, TRunDlthub
from tests.workspace.utils import declare_environment, save_definition, touch_settings


def test_fetch_profile_info(auto_isolated_workspace: WorkspaceRunContext) -> None:
    info = fetch_profile_info()
    assert info is not None
    # the active profile, with absolute paths
    assert info["name"] == "dev"
    assert info["is_current"] is True
    assert os.path.isabs(info["data_dir"])
    assert os.path.isabs(info["local_dir"])
    # `dev` is declared but the empty workspace has no settings files for it
    assert info["configured_profiles"] == []
    assert info["is_configured"] is False
    assert info["location"] == "local"
    # only provider locations that are global or of the active profile
    for prov in info["providers"]:
        for loc in prov["locations"]:
            scope = loc["scope"]
            profile_name = loc.get("profile_name")
            assert (
                scope == "global" or profile_name == "dev"
            ), f"unexpected location {loc} on filtered provider {prov['name']}"
    # `is_pinned` follows `.dlt/profile-name`
    assert info["is_pinned"] is False
    save_profile_pin(auto_isolated_workspace, "dev")
    assert fetch_profile_info()["is_pinned"] is True


def test_print_profile_info(
    auto_isolated_workspace: WorkspaceRunContext,
    capsys: pytest.CaptureFixture[str],
) -> None:
    info = fetch_profile_info()
    assert info is not None
    print_profile_info(info, verbosity=0)
    out = capsys.readouterr().out
    assert "dev" in out
    assert info["data_dir"] in out
    assert info["local_dir"] in out
    # verbose output names the providers
    print_profile_info(info, verbosity=1)
    out = capsys.readouterr().out
    if info["providers"]:
        assert any(p["name"] in out for p in info["providers"])
    # the profile recorded in `.dlt/profile-name` is marked
    save_profile_pin(auto_isolated_workspace, "dev")
    print_profile_info(fetch_profile_info(), verbosity=0)
    assert "Profile is recorded in .dlt/profile-name" in capsys.readouterr().out


def test_print_profile_info_shows_environment(
    auto_isolated_workspace: WorkspaceRunContext,
    capsys: pytest.CaptureFixture[str],
) -> None:
    ctx = auto_isolated_workspace.switch_profile("access")
    info = fetch_profile_info()
    assert info is not None
    # `access` is the default profile with read data access of `prod`
    assert (info["env"], info["data_access"], info.get("default")) == ("prod", "read", True)
    assert info["workspace_name"] == ctx.name and "workspace_id" not in info
    print_profile_info(info, verbosity=0)
    out = capsys.readouterr().out
    assert "Remote profile with" in out and "read data access" in out
    assert "Environment prod" in out and "Other profiles of the environment: prod" in out
    assert f"Remote workspace {ctx.name}, not connected" in out
    connect_remote_workspace(ctx, "prod", workspace_id="prod-id", workspace_name="ingest")
    ctx = ctx.switch_profile("prod")
    print_profile_info(fetch_profile_info(), verbosity=0)
    out = capsys.readouterr().out
    assert "Remote profile with" in out and "write data access" in out
    assert "Environment prod" in out and "Other profiles of the environment: access" in out
    assert "Profile is default" in out
    assert "Remote workspace ingest, id prod-id" in out


def test_fetch_profiles_list_flags(
    auto_isolated_workspace: WorkspaceRunContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv(WORKSPACE__PROFILE, raising=False)
    ctx = auto_isolated_workspace
    profiles = {p["name"]: p for p in fetch_profiles_list()}
    assert set(profiles) == {"dev", "tests", "prod", "access", "no-access"}
    # local profiles belong to no environment, one is the default
    for name in LOCAL_PROFILES:
        assert profiles[name]["location"] == "local", f"{name} should be local"
    assert profiles["dev"].get("default") is True
    assert "env" not in profiles["dev"] and profiles["dev"]["also_in_env"] == []
    # remote profiles belong to an environment, each is the default of its data access
    for name in ("prod", "access", "no-access"):
        assert profiles[name]["location"] == "remote", f"{name} should not be local"
        assert profiles[name].get("default") is True
    assert profiles["access"]["env"] == "prod"
    assert profiles["access"]["data_access"] == "read"
    assert profiles["access"]["also_in_env"] == ["prod", "no-access"]
    assert profiles["prod"]["data_access"] == "write"
    assert profiles["prod"]["also_in_env"] == ["access", "no-access"]
    assert profiles["no-access"]["data_access"] == "none"
    # the default and strict access are set on the environment
    environments = fetch_environments_list()
    assert [e["name"] for e in environments] == ["prod"]
    assert default_profile(environments[0], "write") == "prod"
    assert environments[0].get("default") is True
    assert environments[0].get("strict_access") is False
    assert [p["name"] for p in environments[0]["profile_infos"]] == [
        "prod",
        "access",
        "no-access",
    ]

    # dev is the active profile, none is recorded
    assert profiles["dev"]["is_current"] is True and profiles["dev"]["is_pinned"] is False
    assert profiles["prod"]["is_current"] is False
    # prod recorded in `.dlt/profile-name` is the active profile of a new context
    save_profile_pin(ctx, "prod")
    ctx = switch_context(ctx.run_dir)
    profiles = {p["name"]: p for p in fetch_profiles_list()}
    assert profiles["prod"]["is_pinned"] is True and profiles["prod"]["is_current"] is True
    assert profiles["dev"]["is_pinned"] is False and profiles["dev"]["is_current"] is False

    # a profile with settings files is configured
    assert not any(p["is_configured"] for p in profiles.values())
    touch_settings(ctx, "tests.config.toml")
    assert [p["name"] for p in fetch_profiles_list() if p["is_configured"]] == ["tests"]


def test_list_profiles_groups_local_then_environments(
    auto_isolated_workspace: WorkspaceRunContext,
    capsys: pytest.CaptureFixture[str],
) -> None:
    declare_environment(auto_isolated_workspace, "ci")
    list_profiles(auto_isolated_workspace)
    lines = capsys.readouterr().out.splitlines()

    def index(prefix: str) -> int:
        return next(i for i, line in enumerate(lines) if line.startswith(prefix))

    # local profiles first, then each environment with its profiles indented below it
    assert index("Local profiles:") < index("  * dev ") < index("Remote profiles:")
    assert index("Remote profiles:") < index("  environment prod (default):")
    assert index("  environment prod (default):") < index("    * prod (default)")
    assert index("    * access ") < index("  environment ci (strict about access):")
    assert index("  environment ci") < index("    * ci-write (default)")
    assert lines[index("  * dev ")] == "  * dev (active) (default) - write data access"
    assert lines[index("    * ci-none ")] == "    * ci-none (default) - no data access"
    # an override leaves the recorded profile marked, next to the active one
    save_profile_pin(auto_isolated_workspace, "prod")
    list_profiles(auto_isolated_workspace.switch_profile("tests"))
    out = capsys.readouterr().out
    assert "  * tests (active) - write data access" in out
    assert "    * prod (recorded) (default) - write data access" in out
    assert "  * dev (default) - write data access" in out
    # each environment shows its remote workspace: the auto-name until it is connected
    name = auto_isolated_workspace.name
    assert lines[index("  environment prod") + 1] == f"    remote workspace {name}, not connected"
    assert lines[index("  environment ci") + 1] == f"    remote workspace {name}-ci, not connected"
    connect_remote_workspace(auto_isolated_workspace, "ci", workspace_id="ci-id")
    list_profiles(auto_isolated_workspace)
    lines = capsys.readouterr().out.splitlines()
    assert lines[index("  environment ci") + 1] == f"    remote workspace {name}-ci, id ci-id"


def test_dlthub_profile_without_operation_shows_info(
    auto_isolated_workspace: WorkspaceRunContext, run_dlthub: TRunDlthub
) -> None:
    out = run_dlthub("profile")
    assert "dev" in out and out == run_dlthub("profile", "info")


@pytest.mark.parametrize(
    "argv,expected",
    [
        (["--profile", "prod", "local", "info"], "prod"),
        (["local", "info", "--profile", "prod"], "prod"),
        (["local", "--profile", "prod", "info"], "prod"),
        (["local", "run", "job", "--profile", "prod"], "prod"),
        (["--profile", "prod", "local", "pipeline", "run", "p"], "prod"),
        (["local", "info"], None),
    ],
    ids=["before-command", "after-command", "between", "local-run", "pipeline-run", "absent"],
)
def test_dlthub_profile_option_at_any_position(
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
        ["profile", "list", "--profile", "prod"],
        ["local", "profile", "use", "prod", "--profile", "prod"],
        ["--profile", "prod", "profile", "list"],
        ["--profile", "prod", "local", "profile", "use", "prod"],
    ],
    ids=["after-profile", "after-local-profile", "before-profile", "before-local-profile"],
)
def test_dlthub_profile_commands_take_no_profile_option(
    auto_isolated_workspace: WorkspaceRunContext, main_dlthub: TMainDlthub, argv: List[str]
) -> None:
    code, err = main_dlthub(*argv)
    assert code == 2
    assert "--profile" in err


def test_dlthub_environment_command(
    auto_isolated_workspace: WorkspaceRunContext,
    monkeypatch: pytest.MonkeyPatch,
    main_dlthub: TMainDlthub,
) -> None:
    """A command that takes `--environment` gets no `--profile` and refuses one given before or
    after it."""
    seen: List[argparse.Namespace] = []

    def create_parser(host: str) -> Any:
        parser, pre_parser, installed = _create_parser(host)
        subparsers = next(a for a in parser._actions if isinstance(a, argparse._SubParsersAction))
        add_environment_argument(subparsers.add_parser("deploy"))
        _add_profile_argument(parser)
        installed["deploy"] = SimpleNamespace(execute=seen.append, docs_url=None)  # type: ignore[assignment]
        return parser, pre_parser, installed

    monkeypatch.setattr("dlt._workspace.cli._dlt._create_parser", create_parser)
    assert main_dlthub("deploy", "--environment", "prod")[0] == 0
    assert seen[-1].environment == "prod"
    assert not hasattr(seen[-1], "profile")
    assert main_dlthub("deploy")[0] == 0
    assert seen[-1].environment is None
    # other commands still take --profile
    assert main_dlthub("local", "info", "--profile", "prod")[0] == 0
    for argv in (["deploy", "--profile", "prod"], ["--profile", "prod", "deploy"]):
        code, err = main_dlthub(*argv)
        assert code == 2
        assert "--environment" in err


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
    ):
        code, err = main_dlthub("--profile", name, "local", "info")
        assert code == -1
        assert remedy in err
        # a refused name does not reach the processes the command starts
        assert WORKSPACE__PROFILE not in os.environ
    assert main_dlthub("--profile", "ci-read", "local", "info")[0] == 0
    assert os.environ[WORKSPACE__PROFILE] == "ci-read"


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


def test_dlthub_profile_option_outside_workspace(
    legacy_workspace_context: Any, parse_dlthub: TParseDlthub
) -> None:
    with pytest.raises(SystemExit):
        parse_dlthub(["--profile", "prod", "init"])


def test_dlthub_profile_add(
    auto_isolated_workspace: WorkspaceRunContext, run_dlthub: TRunDlthub
) -> None:
    """`profile add-environment` declares an environment, `profile add` one profile in it or a
    local one. Each shows what it adds and creates, asks, and creates the settings files of the
    profiles that have data access."""
    ctx = auto_isolated_workspace

    def settings(name: str) -> List[bool]:
        return [os.path.isfile(f) for f in profile_settings_files(ctx, name)]

    # declined: nothing is declared or created
    out = run_dlthub("profile", "add-environment", "ci", answer=False)
    assert "dlt changed nothing" in out
    assert "ci" not in ctx.environment_definitions and settings("ci-write") == [False, False]

    out = run_dlthub("profile", "add-environment", "ci")
    # the confirmation names the declaration and every file, then how to add the profile with
    # read data access
    assert f"dlt will add to {os.path.join('.dlt', '.workspace')}:" in out
    assert "environment ci: deployed with ci-write, strict about access" in out
    assert "profile ci-write: environment ci, write data access, default" in out
    after = out.split("dlt will create:\n")[1].splitlines()
    created = [line[len("  * ") :] for line in after if line.startswith("  * ")]
    assert created == [
        os.path.join(".dlt", f"ci-write.{kind}.toml") for kind in ("config", "secrets")
    ]
    assert settings("ci-write") == [True, True]
    assert "Environment ci is strict about access and deploys with ci-write." in out
    assert "dlthub profile add ci-read --access read --env ci" in out
    profiles = {p["name"]: p for p in fetch_profiles_list()}
    assert "ci" not in profiles
    assert profiles["ci-write"]["is_configured"] is True
    # the definition is loaded again once the settings files exist
    assert ctx.get_profile_definition("ci-write")["configured"] is True
    # the profile with read data access that interactive jobs need joins the environment, as
    # the default of its data access
    out = run_dlthub("profile", "add", "ci-read", "--access", "read", "--env", "ci")
    assert "  * profile ci-read: environment ci, read data access, default" in out.splitlines()
    assert settings("ci-read") == [True, True]
    profiles = {p["name"]: p for p in fetch_profiles_list()}
    assert profiles["ci-read"]["env"] == "ci" and profiles["ci-read"]["data_access"] == "read"

    # one profile joins the default environment with write data access unless told otherwise.
    # `prod` stays the default of that data access
    with open(ctx.get_setting("analytics.secrets.toml"), "w", encoding="utf-8") as f:
        f.write("key = 1\n")
    out = run_dlthub("profile", "add", "analytics")
    assert "  * profile analytics: environment prod, write data access" in out.splitlines()
    kept = os.path.join(".dlt", "analytics.secrets.toml")
    assert f"dlt will keep the existing {kept}" in out
    with open(ctx.get_setting("analytics.secrets.toml"), encoding="utf-8") as f:
        assert f.read() == "key = 1\n"
    run_dlthub("profile", "add", "sensitive-access", "--access", "read")
    out = run_dlthub("profile", "add", "ci-audit", "--access", "none", "--env", "ci")
    assert "will create" not in out
    run_dlthub("profile", "add", "lab", "--local")
    profiles = {p["name"]: p for p in fetch_profiles_list()}
    assert (
        profiles["analytics"]["env"] == "prod" and profiles["analytics"]["data_access"] == "write"
    )
    assert (
        profiles["sensitive-access"]["env"] == "prod"
        and profiles["sensitive-access"]["data_access"] == "read"
    )
    assert settings("analytics") == [True, True] and settings("sensitive-access") == [True, True]
    assert settings("ci-audit") == [False, False] and settings("lab") == [True, True]
    assert profiles["lab"]["location"] == "local" and "env" not in profiles["lab"]


@pytest.mark.parametrize(
    "argv,error,match",
    [
        (["add", "x", "--env", "access"], EnvironmentNotDeclared, "`access` is not an environment"),
        (
            ["add", "x", "--env", "staging"],
            EnvironmentNotDeclared,
            "`staging` is not an environment",
        ),
        (["add", "x"], ProfileException, "Name the environment with `--env`: prod, ci"),
        (["add-environment", "ci"], EnvironmentAlreadyExists, "Environment `ci` is already"),
        (["add", "x", "--local", "--env", "prod"], ProfileException, "belongs to no environment"),
        (["add", "x", "--local", "--access", "read"], ProfileException, "write data access"),
    ],
    ids=[
        "profile-as-environment",
        "undeclared-environment",
        "no-default-environment",
        "declared-environment",
        "local-with-environment",
        "local-with-read-access",
    ],
)
def test_dlthub_profile_add_refused(
    auto_isolated_workspace: WorkspaceRunContext,
    run_dlthub: TRunDlthub,
    capsys: pytest.CaptureFixture[str],
    argv: List[str],
    error: Type[Exception],
    match: str,
) -> None:
    """A refusal comes before dlt asks and declares nothing."""
    ctx = auto_isolated_workspace
    run_dlthub("profile", "add-environment", "ci")
    # no environment is the default, so `profile add` needs `--env`
    doc = load_workspace_definition(ctx)
    del doc["environments"]["prod"]["default"]
    save_definition(ctx, doc)
    with pytest.raises(error, match=match):
        run_dlthub("profile", *argv)
    assert "x" not in ctx.profile_definitions
    assert "will add" not in capsys.readouterr().out


def test_dlthub_profile_add_existing(
    auto_isolated_workspace: WorkspaceRunContext,
    run_dlthub: TRunDlthub,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`profile add` for a declared profile or environment creates only the missing settings
    files. When nothing is missing, it shows the declaration and fails."""
    ctx = auto_isolated_workspace

    # `access` is declared without settings files: they are created, the declaration stays
    assert ctx.get_profile_definition("access")["configured"] is False
    declared = load_yaml_file(ctx.get_setting(".workspace"))
    out = run_dlthub("profile", "add", "access", "--env", "prod")
    assert "dlt will create the missing settings files:" in out and "will add" not in out
    assert [os.path.isfile(f) for f in profile_settings_files(ctx, "access")] == [True, True]
    assert load_yaml_file(ctx.get_setting(".workspace")) == declared
    assert ctx.get_profile_definition("access")["configured"] is True

    # nothing is missing: the profile is shown and the command fails. options not given
    # contradict nothing
    with pytest.raises(ProfileAlreadyExists, match="Profile `access` is already declared") as exc:
        run_dlthub("profile", "add", "access")
    assert "differs" not in str(exc.value)
    out = capsys.readouterr().out
    assert "  * access (default) (has settings files) - read data access" in out
    assert "    environment prod, other profiles of the environment: prod" in out
    files = [os.path.join(".dlt", f"access.{kind}.toml") for kind in ("config", "secrets")]
    assert f"settings files: {', '.join(files)}" in out
    # a profile without data access needs no settings files
    run_dlthub("profile", "add-environment", "ci")
    run_dlthub("profile", "add", "ci-none", "--access", "none", "--env", "ci")
    with pytest.raises(ProfileAlreadyExists):
        run_dlthub("profile", "add", "ci-none")
    assert "settings files: none" in capsys.readouterr().out

    # options that contradict the declaration are refused
    with pytest.raises(ProfileAlreadyExists, match="declaration differs from --access write"):
        run_dlthub("profile", "add", "access", "--access", "write")
    with pytest.raises(ProfileAlreadyExists, match="--local, --env ci"):
        run_dlthub("profile", "add", "access", "--local", "--env", "ci")

    # an environment gets back the settings files its profiles miss
    os.remove(profile_settings_files(ctx, "ci-write")[1])
    out = run_dlthub("profile", "add-environment", "ci")
    assert "ci-write.secrets.toml" in out and "ci-write.config.toml" not in out
    assert os.path.isfile(profile_settings_files(ctx, "ci-write")[1])
    with pytest.raises(EnvironmentAlreadyExists):
        run_dlthub("profile", "add-environment", "ci")
    assert "  environment ci (strict about access):" in capsys.readouterr().out
