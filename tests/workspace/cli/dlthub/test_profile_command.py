import os
import sys
from typing import Any, List

import pytest

from dlt._workspace._workspace_context import WorkspaceRunContext
from dlt._workspace.cli._dlt import _create_parser
from dlt._workspace.cli.dlthub._profile_command import print_profile_info
from dlt._workspace.cli.dlthub.utils import fetch_profile_info
from dlt._workspace.profile import save_profile_pin


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
