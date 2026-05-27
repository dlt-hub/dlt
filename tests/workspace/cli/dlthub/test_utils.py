"""Tests for `dlt._workspace.cli.dlthub.utils` — dlthub-host loaders and local-data helpers."""

import os
from typing import Any

import pytest
import yaml
from _pytest.capture import CaptureFixture
from _pytest.monkeypatch import MonkeyPatch

from dlt.common.configuration.specs.pluggable_run_context import RunContextBase
from dlt.common.runtime.run_context import RunContext
from dlt.version import __version__ as dlt_version

from dlt._workspace.cli import echo
from dlt._workspace.cli.dlthub.utils import (
    check_delete_local_data,
    delete_local_data,
    fetch_workspace_info,
)
from dlt._workspace.cli.exceptions import CliCommandException

from tests.workspace.utils import (
    fruitshop_pipeline_context as fruitshop_pipeline_context,
    isolated_workspace,
)


@pytest.mark.parametrize(
    "skip_local_data_dir,recreate_dirs",
    [
        (True, True),
        (True, False),
        (False, True),
        (False, False),
    ],
    ids=[
        "skip-local-data|recreate",
        "skip-local-data|no-recreate",
        "with-local-data|recreate",
        "with-local-data|no-recreate",
    ],
)
def test_delete_local_data_recreate_behavior(
    fruitshop_pipeline_context: RunContextBase,
    capsys: CaptureFixture[str],
    skip_local_data_dir: bool,
    recreate_dirs: bool,
) -> None:
    """verify delete_local_data echoes actions and recreates dirs conditionally.

    the test removes local_dir and data_dir before calling delete_local_data to
    clearly observe the recreate behavior without relying on any previous state.
    """
    ctx = fruitshop_pipeline_context

    # list dirs to delete and auto-confirm
    with echo.always_choose(always_choose_default=False, always_choose_value=True):
        attrs = check_delete_local_data(ctx, skip_local_data_dir=skip_local_data_dir)
    # perform deletion (which will only recreate when requested)
    delete_local_data(ctx, attrs, recreate_dirs=recreate_dirs)

    # data_dir is always processed
    assert os.path.isdir(ctx.data_dir) is recreate_dirs

    # local_dir depends on skip_local_data_dir flag
    expected_local_exists = skip_local_data_dir or recreate_dirs
    assert os.path.isdir(ctx.local_dir) is expected_local_exists

    # capture and check user-facing messages from check_delete_local_data
    out = capsys.readouterr().out
    assert "The following dirs will be deleted:" in out
    assert "(pipeline working folders)" in out
    if skip_local_data_dir:
        assert "(locally loaded data)" not in out
    else:
        assert "(locally loaded data)" in out


def test_delete_local_data_with_plain_run_context_raises(capsys: CaptureFixture[str]) -> None:
    """ensure CliCommandException is raised when context lacks profiles."""
    plain_ctx = RunContext(run_dir=".")
    with pytest.raises(CliCommandException):
        # should fail before any confirmation prompt
        check_delete_local_data(plain_ctx, skip_local_data_dir=False)

    out = capsys.readouterr().out
    assert "ERROR: Cannot delete local data for a context without profiles" in out


def _assert_protected_deletion(
    ctx: RunContextBase,
    capsys: CaptureFixture[str],
    monkeypatch: MonkeyPatch,
    dir_attr: str,
    equals_attr: str,
    *,
    skip_local_data: bool,
) -> None:
    """helper to assert that attempting to delete a protected dir raises and logs an error."""
    # compute target path to match the protected attribute
    target_path = getattr(ctx, equals_attr)

    # patch the property on the class so getattr(ctx, dir_attr) returns the protected path
    monkeypatch.setattr(type(ctx), dir_attr, property(lambda self: target_path), raising=True)

    # exercise and assert
    with pytest.raises(CliCommandException):
        check_delete_local_data(ctx, skip_local_data_dir=skip_local_data)

    out = capsys.readouterr().out
    label = "run dir (workspace root)" if equals_attr == "run_dir" else "settings dir"
    assert f"ERROR: {dir_attr} `{target_path}` is the same as {label} and cannot be deleted" in out


@pytest.mark.parametrize(
    "dir_attr,equals_attr,skip_local_data",
    [
        ("local_dir", "run_dir", False),
        ("local_dir", "settings_dir", False),
        ("data_dir", "run_dir", True),
        ("data_dir", "settings_dir", True),
    ],
    ids=[
        "local_dir==run_dir",
        "local_dir==settings_dir",
        "data_dir==run_dir",
        "data_dir==settings_dir",
    ],
)
def test_delete_local_data_protects_run_and_settings_dirs(
    fruitshop_pipeline_context: RunContextBase,
    capsys: CaptureFixture[str],
    monkeypatch: MonkeyPatch,
    dir_attr: str,
    equals_attr: str,
    skip_local_data: bool,
) -> None:
    """verify that delete_local_data refuses to delete run_dir or settings_dir via local/data dir.

    we patch the context so the target dir equals a protected dir and expect a CliCommandException.
    """
    _assert_protected_deletion(
        fruitshop_pipeline_context,
        capsys,
        monkeypatch,
        dir_attr,
        equals_attr,
        skip_local_data=skip_local_data,
    )


def test_delete_local_data_rejects_dirs_outside_run_dir(
    fruitshop_pipeline_context: RunContextBase,
    capsys: CaptureFixture[str],
    monkeypatch: MonkeyPatch,
    tmp_path: Any,
) -> None:
    """ensure we refuse to operate on dirs that are not under the workspace run_dir."""
    ctx = fruitshop_pipeline_context

    # point local_dir to a path outside of run_dir
    outside_dir = tmp_path / "outside_local"
    outside_dir.mkdir(parents=True, exist_ok=True)

    # patch attribute to simulate unsafe location

    monkeypatch.setattr(
        type(ctx), "local_dir", property(lambda self: str(outside_dir)), raising=True
    )

    with pytest.raises(CliCommandException):
        check_delete_local_data(ctx, skip_local_data_dir=False)

    out = capsys.readouterr().out
    assert (
        f"ERROR: local_dir `{ctx.local_dir}` is not within run dir (workspace root) and cannot be"
        " deleted"
        in out
    )


def test_fetch_workspace_info_has_dlt_fields() -> None:
    """fetch_workspace_info returns dlt_version, dlthub_version, initialized, installed_toolkits."""
    with isolated_workspace("empty"):
        info = fetch_workspace_info()

    assert info["dlt_version"] == dlt_version
    # dlthub_version is None unless the dlthub package is installed
    assert "dlthub_version" in info
    # initialized depends on config.toml presence
    assert isinstance(info["initialized"], bool)
    # installed_toolkits is a dict (may be empty)
    assert isinstance(info["installed_toolkits"], dict)


def test_fetch_workspace_info_initialized_flag() -> None:
    """initialized is True when config.toml exists, False otherwise."""
    with isolated_workspace("empty") as ctx:
        # no config.toml yet
        info = fetch_workspace_info()
        assert info["initialized"] is False

        # create config.toml
        config_path = os.path.join(ctx.settings_dir, "config.toml")
        os.makedirs(os.path.dirname(config_path), exist_ok=True)
        with open(config_path, "w", encoding="utf-8") as f:
            f.write("[runtime]\n")

        info = fetch_workspace_info()
        assert info["initialized"] is True


def test_fetch_workspace_info_installed_toolkits() -> None:
    """installed_toolkits reflects the .dlt/.toolkits index."""

    with isolated_workspace("empty") as ctx:
        # initially empty
        info = fetch_workspace_info()
        assert info["installed_toolkits"] == {}

        # write a toolkit entry
        toolkits_path = os.path.join(ctx.settings_dir, ".toolkits")
        os.makedirs(os.path.dirname(toolkits_path), exist_ok=True)
        entry = {
            "my-tk": {
                "version": "1.0.0",
                "installed_at": "2025-01-01T00:00:00+00:00",
                "agent": "claude",
                "description": "My toolkit",
                "tags": ["test"],
            }
        }
        with open(toolkits_path, "w", encoding="utf-8") as f:
            yaml.dump(entry, f)

        info = fetch_workspace_info()
        assert "my-tk" in info["installed_toolkits"]
        assert info["installed_toolkits"]["my-tk"]["version"] == "1.0.0"
        assert info["installed_toolkits"]["my-tk"]["description"] == "My toolkit"
        assert info["installed_toolkits"]["my-tk"]["tags"] == ["test"]
