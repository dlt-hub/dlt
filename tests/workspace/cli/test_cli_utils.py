import os
import shutil
from typing import Literal

import pytest
from _pytest.capture import CaptureFixture
from _pytest.monkeypatch import MonkeyPatch

from dlt._workspace.cli.utils import delete_local_data
from dlt._workspace.cli.exceptions import CliCommandException
from dlt.common.runtime.run_context import RunContext
from dlt.common.configuration.specs.pluggable_run_context import RunContextBase

# import the same pokemon fixture as in test_mcp_tools
from tests.workspace.utils import fruitshop_pipeline_context as fruitshop_pipeline_context


def _remove_dir(path: str) -> None:
    """remove a directory tree if it exists."""
    if os.path.isdir(path):
        shutil.rmtree(path)


@pytest.mark.parametrize(
    "skip_data_dir,recreate_dirs",
    [
        (True, True),
        (True, False),
        (False, True),
        (False, False),
    ],
    ids=[
        "skip-data|recreate",
        "skip-data|no-recreate",
        "with-data|recreate",
        "with-data|no-recreate",
    ],
)
def test_delete_local_data_recreate_behavior(
    fruitshop_pipeline_context: RunContextBase,
    capsys: CaptureFixture[str],
    skip_data_dir: bool,
    recreate_dirs: bool,
) -> None:
    """verify delete_local_data echoes actions and recreates dirs conditionally.

    the test removes local_dir and data_dir before calling delete_local_data to
    clearly observe the recreate behavior without relying on any previous state.
    """
    ctx = fruitshop_pipeline_context

    # call function under test
    delete_local_data(ctx, skip_data_dir=skip_data_dir, recreate_dirs=recreate_dirs)

    # local_dir is always processed
    assert os.path.isdir(ctx.local_dir) is recreate_dirs

    # data_dir depends on skip_data_dir flag
    expected_data_exists = skip_data_dir or recreate_dirs
    assert os.path.isdir(ctx.data_dir) is expected_data_exists

    # capture and check user-facing messages (ignore styled path)
    out = capsys.readouterr().out
    assert "Will delete locally loaded data in " in out
    if skip_data_dir:
        assert "Will delete pipeline working folders & other entities data " not in out
    else:
        assert "Will delete pipeline working folders & other entities data " in out


def test_delete_local_data_with_plain_run_context_raises(capsys: CaptureFixture[str]) -> None:
    """ensure CliCommandException is raised when context lacks profiles."""
    plain_ctx = RunContext(run_dir=".")
    with pytest.raises(CliCommandException):
        delete_local_data(plain_ctx, skip_data_dir=False, recreate_dirs=True)

    out = capsys.readouterr().out
    assert "ERROR: Cannot delete local data for a context without profiles" in out


def _assert_protected_deletion(
    ctx: RunContextBase,
    capsys: CaptureFixture[str],
    monkeypatch: MonkeyPatch,
    dir_attr: str,
    equals_attr: str,
    *,
    skip_data: bool,
) -> None:
    """helper to assert that attempting to delete a protected dir raises and logs an error.

    Args:
        ctx: Workspace run context fixture instance.
        capsys: pytest capsys to capture output.
        monkeypatch: pytest monkeypatch fixture to patch properties.
        dir_attr: Attribute to delete ("local_dir" or "data_dir").
        equals_attr: Attribute to match against ("run_dir" or "settings_dir").
        skip_data: Whether to skip data_dir deletion in delete_local_data.
    """
    # compute target path to match the protected attribute
    target_path = getattr(ctx, equals_attr)

    # patch the property on the class so getattr(ctx, dir_attr) returns the protected path
    monkeypatch.setattr(type(ctx), dir_attr, property(lambda self: target_path), raising=True)

    # exercise and assert
    with pytest.raises(CliCommandException):
        delete_local_data(ctx, skip_data_dir=skip_data, recreate_dirs=True)

    out = capsys.readouterr().out
    label = "run dir (workspace root)" if equals_attr == "run_dir" else "settings dir"
    assert f"ERROR: {dir_attr} `deleted_dir` is the same as {label} and cannot be deleted" in out


@pytest.mark.parametrize(
    "dir_attr,equals_attr,skip_data",
    [
        ("local_dir", "run_dir", True),
        ("local_dir", "settings_dir", True),
        ("data_dir", "run_dir", False),
        ("data_dir", "settings_dir", False),
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
    skip_data: bool,
) -> None:
    """verify that delete_local_data refuses to delete run_dir or settings_dir via local/data dir.

    we patch the context so the target dir equals a protected dir and expect a CliCommandException.
    """
    _assert_protected_deletion(
        fruitshop_pipeline_context, capsys, monkeypatch, dir_attr, equals_attr, skip_data=skip_data
    )
