import os
from pathlib import Path

import pytest

from dlt._workspace.deployment.file_selector import (
    ConfigurationFileSelector,
    DEFAULT_IGNORES,
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


def test_default_ignores_applied_without_ignore_file() -> None:
    """DEFAULT_IGNORES patterns are applied when no ignore file exists."""
    with isolated_workspace("default") as ctx:
        root = Path(ctx.run_dir)
        # create directories matching DEFAULT_IGNORES
        for dirname in [
            ".venv",
            "venv",
            "dist",
            "build",
            "my_pkg.egg-info",
            ".mypy_cache",
            ".ruff_cache",
            ".pytest_cache",
            "htmlcov",
        ]:
            d = root / dirname
            d.mkdir(exist_ok=True)
            (d / "somefile").write_text("x")

        # create files matching DEFAULT_IGNORES
        (root / "module.pyc").write_text("x")
        (root / "module.pyo").write_text("x")
        (root / "module.pyd").write_text("x")
        (root / ".coverage").write_text("x")
        (root / "extension.so").write_text("x")
        (root / ".DS_Store").write_text("x")
        (root / ".env").write_text("x")

        # use default ignore_file (.gitignore) which does not exist in this workspace
        selector = WorkspaceFileSelector(ctx)
        files = {rel.as_posix() for _, rel in selector}

        # none of the default-ignored paths should appear
        for name in [
            ".venv/somefile",
            "venv/somefile",
            "dist/somefile",
            "build/somefile",
            "my_pkg.egg-info/somefile",
            ".mypy_cache/somefile",
            ".ruff_cache/somefile",
            ".pytest_cache/somefile",
            "htmlcov/somefile",
            "module.pyc",
            "module.pyo",
            "module.pyd",
            ".coverage",
            "extension.so",
            ".DS_Store",
            ".env",
        ]:
            assert name not in files, f"{name} should be excluded by DEFAULT_IGNORES"

        # regular workspace files should still be present
        assert "ducklake_pipeline.py" in files
        assert "empty_file.py" in files


def test_default_ignores_not_applied_with_ignore_file() -> None:
    """DEFAULT_IGNORES patterns are NOT applied when an ignore file exists."""
    with isolated_workspace("default") as ctx:
        root = Path(ctx.run_dir)
        # create a directory that would be excluded by DEFAULT_IGNORES
        venv_dir = root / ".venv"
        venv_dir.mkdir(exist_ok=True)
        (venv_dir / "somefile").write_text("x")

        # .ignorefile exists and only excludes /empty_file.py
        selector = WorkspaceFileSelector(ctx, ignore_file=".ignorefile")
        files = {rel.as_posix() for _, rel in selector}

        # .venv/ is NOT in .ignorefile, so it should appear (DEFAULT_IGNORES not used)
        assert ".venv/somefile" in files


def test_configuration_file_selector() -> None:
    """Test that ConfigurationFileSelector yields only config/secrets from settings dir."""
    with isolated_workspace("configured_workspace") as ctx:
        selector = ConfigurationFileSelector(ctx)
        files = {rel.as_posix() for _, rel in selector}
        # In this workspace case only .config.toml files exist
        assert files == {"config.toml", "dev.config.toml"}
