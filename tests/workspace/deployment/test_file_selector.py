import os
import shutil
from pathlib import Path
from typing import Any, List, Optional, Set

import pytest

from dlt._workspace._workspace_context import WorkspaceRunContext
from dlt._workspace.cli.dlthub.ai.utils import TOOLKITS_INDEX_FILE
from dlt._workspace.deployment.file_selector import (
    DEFAULT_IGNORES,
    GitignoreFileSelector,
    WorkspaceFileSelector,
)

from tests.workspace.manifest_utils import shipped_settings
from tests.workspace.utils import declare_environment, isolated_workspace


@pytest.mark.parametrize(
    "with_additional_exclude",
    [True, False],
    ids=["with_additional_exclude", "without_additional_exclude"],
)
def test_file_selector_respects_gitignore(with_additional_exclude: bool) -> None:
    """Test that .gitignore patterns are respected with and without additional excludes."""

    additional_excludes = ["additional_exclude/"] if with_additional_exclude else None
    # the profiles declared in `.workspace` ship with the code
    expected_files = {
        "additional_exclude/empty_file.py",
        "ducklake_pipeline.py",
        ".ignorefile",
        ".dlt/.workspace",
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


def test_file_selector_does_not_follow_symlink_loops() -> None:
    """A symlink pointing at one of its own parents must not make iteration recurse."""
    with isolated_workspace("default") as ctx:
        root = Path(ctx.run_dir)
        looping_dir = root / "looping"
        looping_dir.mkdir(exist_ok=True)
        (looping_dir / "somefile.py").write_text("x")
        os.symlink(looping_dir, looping_dir / "self")

        selector = WorkspaceFileSelector(ctx, ignore_file=".ignorefile")
        files = {rel.as_posix() for _, rel in selector}

        assert "looping/somefile.py" in files
        assert "ducklake_pipeline.py" in files


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


SHARED_SETTINGS = ["config.toml", "secrets.toml"]
PROD_SETTINGS = [
    "access.config.toml",
    "access.secrets.toml",
    "prod.config.toml",
    "prod.secrets.toml",
]
# `staging-read` has secrets only and `staging-none` no settings file at all
STAGING_SETTINGS = [
    "staging-read.secrets.toml",
    "staging-write.config.toml",
    "staging-write.secrets.toml",
]
NEVER_SHIPPED = [
    "dev.config.toml",
    "dev.secrets.toml",
    "tests.config.toml",
    "tests.secrets.toml",
    "analytics.config.toml",
    "analytics.secrets.toml",
    "myconfig.toml",
    "README.md",
]
"""Files of the local profiles, of the undeclared `analytics`, and of no profile at all."""


def _configure_environments(ctx: WorkspaceRunContext) -> None:
    """Declares `staging` next to the built-in `prod` and creates the settings files above."""
    declare_environment(ctx, "staging")
    # `analytics` gets its files after the migration of the workspace, so nothing declares it
    for name in SHARED_SETTINGS + PROD_SETTINGS + STAGING_SETTINGS + NEVER_SHIPPED:
        Path(ctx.settings_dir, name).touch()


@pytest.mark.parametrize(
    "environment,profile_settings,other_settings",
    [("prod", PROD_SETTINGS, STAGING_SETTINGS), ("staging", STAGING_SETTINGS, PROD_SETTINGS)],
    ids=["built-in-prod", "staging"],
)
def test_configuration_file_selector(
    environment: str, profile_settings: List[str], other_settings: List[str]
) -> None:
    """A deployment ships the shared settings files and the settings files of the profiles of its
    environment, sorted by name. `.workspace` ships with the code."""
    with isolated_workspace("configured_workspace") as ctx:
        _configure_environments(ctx)
        assert "analytics" not in ctx.profile_definitions
        files = shipped_settings(ctx, ctx.get_environment_definition(environment))

    assert files == sorted(SHARED_SETTINGS + profile_settings)
    # the settings of the other environment, of local and of undeclared profiles never ship
    assert set(files).isdisjoint(other_settings + NEVER_SHIPPED)


def test_toolkits_index_ships_but_secrets_do_not() -> None:
    """The agent launcher resolves `<toolkit>:<agent>` from the index, so it must reach the runner."""
    with isolated_workspace("default") as ctx:
        settings = Path(ctx.settings_dir)
        settings.mkdir(parents=True, exist_ok=True)
        (settings / TOOLKITS_INDEX_FILE).write_text("dlthub-platform:\n  version: '0.2.0'\n")
        (settings / "secrets.toml").write_text("[destination]\n")
        (settings / "config.toml").write_text("[runtime]\n")
        (settings / "state").mkdir(exist_ok=True)
        (settings / "state" / "local.duckdb").write_text("x")

        files = {rel.as_posix() for _, rel in WorkspaceFileSelector(ctx, ignore_file=".ignorefile")}

    assert f".dlt/{TOOLKITS_INDEX_FILE}" in files
    # everything else under the settings dir stays out, secrets above all
    assert ".dlt/secrets.toml" not in files
    assert ".dlt/config.toml" not in files
    assert ".dlt/state/local.duckdb" not in files


def test_gitignore_selector_never_enters_ignored_folders(monkeypatch: pytest.MonkeyPatch) -> None:
    """`.git` is always out, ignored folders are not walked, `include` narrows what is left."""
    walked: List[str] = []
    walk = os.walk

    def recording_walk(top: Any, *args: Any, **kwargs: Any) -> Any:
        for entry in walk(top, *args, **kwargs):
            walked.append(Path(entry[0]).name)
            yield entry

    with isolated_workspace("default") as ctx:
        root = Path(ctx.run_dir)
        (root / ".venv" / "lib").mkdir(parents=True)
        (root / ".venv" / "lib" / "dep.py").write_text("x")
        (root / ".git").mkdir()
        (root / ".git" / "config").write_text("x")
        (root / ".gitignore").write_text(".venv/\n")

        monkeypatch.setattr(os, "walk", recording_walk)
        selected = [rel.as_posix() for _, rel in GitignoreFileSelector(ctx.run_dir)]
        monkeypatch.undo()
        again = [rel.as_posix() for _, rel in GitignoreFileSelector(ctx.run_dir)]
        only_py = {
            rel.as_posix() for _, rel in GitignoreFileSelector(ctx.run_dir, include=["*.py"])
        }
        # without the ignore file the virtualenv comes back, git internals never do
        everything = {
            rel.as_posix() for _, rel in GitignoreFileSelector(ctx.run_dir, use_ignore_file=False)
        }

    assert {".gitignore", "ducklake_pipeline.py", "additional_exclude/empty_file.py"} <= set(
        selected
    )
    assert not any(path.startswith((".venv/", ".git/")) for path in selected)
    assert ".venv" not in walked and ".git" not in walked
    # files come in the same order every time, so a page of them is the same page
    assert selected == again
    assert {"ducklake_pipeline.py", "additional_exclude/empty_file.py"} <= only_py
    assert all(path.endswith(".py") for path in only_py)
    assert ".venv/lib/dep.py" in everything and ".git/config" not in everything


IGNORE_PATTERNS = """data/
!data/keep.csv
*.log
!important.log
logs/
!keep.txt
/build
nested/**/skip.py
cache
"""

PACKAGED_FILES = {
    ".gitignore",
    ".git/HEAD",
    "data/keep.csv",
    "data/sub/keep.txt",
    "logs/keep.txt",
    "important.log",
    "src/build/kept.py",
    "nested/a/b/ok.py",
    "plain.py",
    "extra/e.py",
    "__pycache__/m.pyc",
    ".venv/lib/x.py",
    "link_dir",
    "link_file.py",
}
"""What a deployment ships from the files below, given `IGNORE_PATTERNS` as the ignore file."""


@pytest.mark.parametrize(
    "additional_excludes,left_out",
    [(None, set()), (["extra/", "*.csv"], {"extra/e.py", "data/keep.csv"})],
    ids=["ignore-file", "additional-excludes"],
)
def test_workspace_selector_applies_gitignore_semantics(
    additional_excludes: Optional[List[str]], left_out: Set[str]
) -> None:
    """Folder and file patterns, anchors, `**`, negations inside excluded folders and symlinks."""
    with isolated_workspace("default") as ctx:
        root = Path(ctx.run_dir)
        for template_file in ("ducklake_pipeline.py", "empty_file.py", ".ignorefile"):
            (root / template_file).unlink()
        shutil.rmtree(root / "additional_exclude")
        for relative in [
            "data/a.csv",
            "data/keep.csv",
            "data/sub/keep.txt",
            "logs/x.log",
            "logs/keep.txt",
            "app.log",
            "important.log",
            "build/out.bin",
            "src/build/kept.py",
            "nested/a/b/skip.py",
            "nested/a/b/ok.py",
            "cache/c.bin",
            "src/cache",
            "plain.py",
            ".git/HEAD",
            "extra/e.py",
            "__pycache__/m.pyc",
            ".venv/lib/x.py",
        ]:
            (root / relative).parent.mkdir(parents=True, exist_ok=True)
            (root / relative).write_text("x")
        (root / ".gitignore").write_text(IGNORE_PATTERNS)
        os.symlink(root / "src", root / "link_dir")
        os.symlink(root / "plain.py", root / "link_file.py")
        os.symlink(root / "missing", root / "dangling")

        files = {
            rel.as_posix()
            for _, rel in WorkspaceFileSelector(ctx, additional_excludes=additional_excludes)
        }

    # the settings dir stays out, `.git` ships unless the ignore file says otherwise, a symlinked
    # folder ships as a link and a broken link does not
    assert {path for path in files if not path.startswith(".dlt/")} == PACKAGED_FILES - left_out
