import os
from pathlib import Path

import pytest

from dlt._workspace.deployment.file_selector import (
    ConfigurationFileSelector,
    WorkspaceFileSelector,
)

from tests.workspace.utils import isolated_workspace


@pytest.mark.parametrize(
    "with_additional_exclude",
    [True, False],
    ids=["with_additional_exclude", "without_additional_exclude"],
)
def test_file_selector_respects_gitignore(with_additional_exclude: bool) -> None:
    """Test that .gitignore patterns are respected with and without additional excludes."""

    additional_excludes = ["additional_exclude/"] if with_additional_exclude else None
    expected_files = {
        "additional_exclude/empty_file.py",
        "ducklake_pipeline.py",
        ".ignorefile",
    }
    if with_additional_exclude:
        expected_files.remove("additional_exclude/empty_file.py")

    with isolated_workspace("default") as ctx:
        selector = WorkspaceFileSelector(
            ctx, additional_excludes=additional_excludes, ignore_file=".ignorefile"
        )
        files = {rel.as_posix() for _, rel in selector}
        assert files == expected_files


def test_file_selector_ignore_file_not_found() -> None:
    """ignore_file_found is False when the configured ignore file does not exist."""
    with isolated_workspace("default") as ctx:
        selector = WorkspaceFileSelector(ctx, ignore_file=".nonexistent_ignore")
    assert selector.ignore_file_found is False


def test_file_selector_ignore_file_found() -> None:
    """ignore_file_found is True when the configured ignore file exists."""
    with isolated_workspace("default") as ctx:
        selector = WorkspaceFileSelector(ctx, ignore_file=".ignorefile")
    assert selector.ignore_file_found is True


def test_default_excludes_without_gitignore() -> None:
    """Default excludes filter well-known non-deployable dirs even without .gitignore."""
    with isolated_workspace("default") as ctx:
        root = Path(ctx.run_dir)
        # create directories that should be excluded by default
        for dirname in [".git", ".venv", "__pycache__", "node_modules", ".mypy_cache"]:
            d = root / dirname
            d.mkdir(exist_ok=True)
            (d / "somefile").write_text("x")
        # also create a .pyc file at root level
        (root / "compiled.pyc").write_text("x")

        # use default ignore_file (.gitignore) which does not exist in this workspace
        selector = WorkspaceFileSelector(ctx)
        files = {rel.as_posix() for _, rel in selector}

        # none of the default-excluded paths should appear
        for name in [
            ".git/somefile",
            ".venv/somefile",
            "__pycache__/somefile",
            "node_modules/somefile",
            ".mypy_cache/somefile",
            "compiled.pyc",
        ]:
            assert name not in files, f"{name} should be excluded by default"

        # regular workspace files should still be present
        assert "ducklake_pipeline.py" in files
        assert "empty_file.py" in files


def test_configuration_file_selector() -> None:
    """Test that ConfigurationFileSelector yields only config/secrets from settings dir."""
    with isolated_workspace("configured_workspace") as ctx:
        selector = ConfigurationFileSelector(ctx)
        files = {rel.as_posix() for _, rel in selector}
        # In this workspace case only .config.toml files exist
        assert files == {"config.toml", "dev.config.toml"}
