import os
import sys
from pathlib import Path
from typing import Any, List

import pytest

from dlt._workspace._workspace_context import WorkspaceRunContext
from dlt._workspace.cli._dlt import _create_parser
from dlt._workspace.cli._profile_command import (
    clean_profile,
    print_profile_info,
)
from dlt._workspace.cli.exceptions import CliCommandException
from dlt._workspace.cli.utils import fetch_profile_info
from dlt._workspace.profile import save_profile_pin


# fetch_profile_info() unit tests


def test_fetch_profile_info_returns_current(
    auto_isolated_workspace: WorkspaceRunContext,
) -> None:
    info = fetch_profile_info()
    assert info is not None
    assert info["name"] == "dev"
    assert info["is_current"] is True


def test_fetch_profile_info_paths_absolute(
    auto_isolated_workspace: WorkspaceRunContext,
) -> None:
    info = fetch_profile_info()
    assert info is not None
    assert os.path.isabs(info["data_dir"])
    assert os.path.isabs(info["local_dir"])


def test_fetch_profile_info_pinned_when_pin_file_exists(
    auto_isolated_workspace: WorkspaceRunContext,
) -> None:
    save_profile_pin(auto_isolated_workspace, "dev")
    info = fetch_profile_info()
    assert info is not None
    assert info["is_pinned"] is True


def test_fetch_profile_info_not_pinned_when_pin_file_absent(
    auto_isolated_workspace: WorkspaceRunContext,
) -> None:
    info = fetch_profile_info()
    assert info is not None
    assert info["is_pinned"] is False


def test_fetch_profile_info_includes_configured_profiles(
    auto_isolated_workspace: WorkspaceRunContext,
) -> None:
    info = fetch_profile_info()
    assert info is not None
    # `dev` is the active profile, so it's always configured
    assert "dev" in info["configured_profiles"]


def test_fetch_profile_info_providers_filter_global_or_current(
    auto_isolated_workspace: WorkspaceRunContext,
) -> None:
    # filtered providers must contain only locations whose scope is global or
    # whose profile_name matches the active profile
    info = fetch_profile_info()
    assert info is not None
    for prov in info["providers"]:
        for loc in prov["locations"]:
            scope = loc["scope"]
            profile_name = loc.get("profile_name")
            assert (
                scope == "global" or profile_name == "dev"
            ), f"unexpected location {loc} on filtered provider {prov['name']}"


# print_profile_info() view tests


def test_print_profile_info_basic(
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


def test_print_profile_info_verbose_lists_providers(
    auto_isolated_workspace: WorkspaceRunContext,
    capsys: pytest.CaptureFixture[str],
) -> None:
    info = fetch_profile_info()
    assert info is not None
    print_profile_info(info, verbosity=1)
    out = capsys.readouterr().out
    if info["providers"]:
        # at least one provider name appears in verbose output
        assert any(p["name"] in out for p in info["providers"])


def test_print_profile_info_pinned_marker(
    auto_isolated_workspace: WorkspaceRunContext,
    capsys: pytest.CaptureFixture[str],
) -> None:
    save_profile_pin(auto_isolated_workspace, "dev")
    info = fetch_profile_info()
    assert info is not None
    print_profile_info(info, verbosity=0)
    out = capsys.readouterr().out
    assert "pinned" in out.lower()


# clean_profile() tests


def test_clean_profile_current_deletes_dirs(
    auto_isolated_workspace: WorkspaceRunContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # populate data_dir and local_dir
    Path(auto_isolated_workspace.data_dir).mkdir(parents=True, exist_ok=True)
    Path(auto_isolated_workspace.local_dir).mkdir(parents=True, exist_ok=True)
    (Path(auto_isolated_workspace.data_dir) / "marker.txt").write_text("x")
    (Path(auto_isolated_workspace.local_dir) / "marker.txt").write_text("x")

    # auto-confirm prompts
    from dlt._workspace.cli import echo as fmt

    monkeypatch.setattr(fmt, "ALWAYS_CONFIRM", True)

    clean_profile(auto_isolated_workspace, profile_name=None, skip_data_dir=False)

    # `delete_local_data` recreates the dirs empty after wiping; the markers must be gone
    assert not (Path(auto_isolated_workspace.data_dir) / "marker.txt").exists()
    assert not (Path(auto_isolated_workspace.local_dir) / "marker.txt").exists()


def test_clean_profile_skip_data_dir_preserves_data(
    auto_isolated_workspace: WorkspaceRunContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    Path(auto_isolated_workspace.data_dir).mkdir(parents=True, exist_ok=True)
    Path(auto_isolated_workspace.local_dir).mkdir(parents=True, exist_ok=True)
    (Path(auto_isolated_workspace.data_dir) / "marker.txt").write_text("x")
    (Path(auto_isolated_workspace.local_dir) / "marker.txt").write_text("x")

    from dlt._workspace.cli import echo as fmt

    monkeypatch.setattr(fmt, "ALWAYS_CONFIRM", True)

    clean_profile(auto_isolated_workspace, profile_name=None, skip_data_dir=True)

    # data_dir contents preserved; local_dir wiped (recreated empty)
    assert (Path(auto_isolated_workspace.data_dir) / "marker.txt").exists()
    assert not (Path(auto_isolated_workspace.local_dir) / "marker.txt").exists()


def test_clean_profile_named_switches_context(
    auto_isolated_workspace: WorkspaceRunContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # active profile is `dev`. clean `tests` (a built-in profile) — different ctx.
    tests_ctx = auto_isolated_workspace.switch_profile("tests")
    Path(tests_ctx.data_dir).mkdir(parents=True, exist_ok=True)
    Path(tests_ctx.local_dir).mkdir(parents=True, exist_ok=True)
    (Path(tests_ctx.data_dir) / "marker.txt").write_text("tests-data")

    # populate the active dev profile too — must be unaffected
    Path(auto_isolated_workspace.data_dir).mkdir(parents=True, exist_ok=True)
    (Path(auto_isolated_workspace.data_dir) / "dev-marker.txt").write_text("dev-data")

    from dlt._workspace.cli import echo as fmt

    monkeypatch.setattr(fmt, "ALWAYS_CONFIRM", True)

    clean_profile(auto_isolated_workspace, profile_name="tests", skip_data_dir=False)

    # tests profile contents wiped
    assert not (Path(tests_ctx.data_dir) / "marker.txt").exists()
    # dev profile data is preserved
    assert (Path(auto_isolated_workspace.data_dir) / "dev-marker.txt").exists()


def test_clean_profile_unknown_profile_raises(
    auto_isolated_workspace: WorkspaceRunContext,
) -> None:
    with pytest.raises(CliCommandException):
        clean_profile(auto_isolated_workspace, profile_name="nonexistent", skip_data_dir=False)


def test_clean_profile_user_declines_confirmation(
    auto_isolated_workspace: WorkspaceRunContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    Path(auto_isolated_workspace.data_dir).mkdir(parents=True, exist_ok=True)
    (Path(auto_isolated_workspace.data_dir) / "marker.txt").write_text("x")

    from dlt._workspace.cli import echo as fmt

    # decline both prompts
    monkeypatch.setattr(fmt, "ALWAYS_CHOOSE_DEFAULT", True)

    clean_profile(auto_isolated_workspace, profile_name=None, skip_data_dir=False)

    # default for the confirmation prompt is False → nothing deleted
    assert (Path(auto_isolated_workspace.data_dir) / "marker.txt").exists()


# dlthub profile argparse routing tests


def _build_dlthub_parser(monkeypatch: pytest.MonkeyPatch, argv: List[str]) -> Any:
    monkeypatch.setattr(sys, "argv", ["dlthub", *argv])
    parser, _pre, _installed = _create_parser("dlthub")
    return parser


def _parse_dlthub(monkeypatch: pytest.MonkeyPatch, argv: List[str]) -> Any:
    """Run the dual-parse (pre-parser + main parser) and return parsed args."""
    monkeypatch.setattr(sys, "argv", ["dlthub", *argv])
    parser, pre_parser, _installed = _create_parser("dlthub")
    ns, remaining = pre_parser.parse_known_args(argv)
    return parser.parse_args(remaining, namespace=ns)


def test_dlthub_profile_default_to_info_parses(
    auto_isolated_workspace: WorkspaceRunContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    args = _parse_dlthub(monkeypatch, ["profile"])
    assert args.command == "profile"
    assert getattr(args, "operation", None) is None


def test_dlthub_profile_info_parses(
    auto_isolated_workspace: WorkspaceRunContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    args = _parse_dlthub(monkeypatch, ["profile", "info"])
    assert args.operation == "info"


def test_dlthub_profile_list_parses(
    auto_isolated_workspace: WorkspaceRunContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    args = _parse_dlthub(monkeypatch, ["profile", "list"])
    assert args.operation == "list"


def test_dlthub_profile_use_with_name(
    auto_isolated_workspace: WorkspaceRunContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    args = _parse_dlthub(monkeypatch, ["profile", "use", "dev"])
    assert args.operation == "use"
    assert args.profile_name == "dev"


def test_dlthub_profile_use_without_name_errors(
    auto_isolated_workspace: WorkspaceRunContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    parser = _build_dlthub_parser(monkeypatch, ["profile", "use"])
    with pytest.raises(SystemExit):
        parser.parse_args(["profile", "use"])


def test_dlthub_profile_clean_removed(
    auto_isolated_workspace: WorkspaceRunContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    # `clean` moved from profile to `dlthub local clean`
    parser = _build_dlthub_parser(monkeypatch, ["profile", "clean"])
    with pytest.raises(SystemExit):
        parser.parse_args(["profile", "clean"])


def test_dlthub_profile_pin_removed(
    auto_isolated_workspace: WorkspaceRunContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    # `pin` was renamed to `use` — `pin` is no longer a valid subcommand
    parser = _build_dlthub_parser(monkeypatch, ["profile", "pin", "dev"])
    with pytest.raises(SystemExit):
        parser.parse_args(["profile", "pin", "dev"])
