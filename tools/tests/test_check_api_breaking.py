"""Tests for tools/check_api_breaking.py."""

from pathlib import Path
from types import SimpleNamespace
from typing import Any, Type

import pytest

import tools.check_api_breaking as mod
from tools.check_api_breaking import PUBLIC_API_ROOTS, _breakage_source_file, public_api
from tools.git_utils import detect_base_ref


def _breakage(obj: Any) -> Any:
    """Minimal stand-in for a griffe `Breakage` carrying the affected object."""
    return SimpleNamespace(obj=obj)


def test_breakage_source_file_direct() -> None:
    obj = SimpleNamespace(
        is_alias=False, relative_package_filepath=Path("dlt/pipeline/pipeline.py")
    )
    assert _breakage_source_file(_breakage(obj)) == "dlt/pipeline/pipeline.py"


def test_breakage_source_file_alias_uses_parent() -> None:
    """An alias is attributed to its parent's file, not its own."""
    parent = SimpleNamespace(relative_package_filepath=Path("dlt/__init__.py"))
    alias = SimpleNamespace(is_alias=True, parent=parent)
    assert _breakage_source_file(_breakage(alias)) == "dlt/__init__.py"


@pytest.mark.parametrize("exc", [ValueError, AttributeError])
def test_breakage_source_file_without_filepath_returns_none(exc: Type[Exception]) -> None:
    class _NoFile:
        is_alias = False

        @property
        def relative_package_filepath(self) -> Path:
            raise exc("no filepath")

    assert _breakage_source_file(_breakage(_NoFile())) is None


def test_public_api_reports_roots_and_sources() -> None:
    root_to_names, source_files = public_api()
    assert set(root_to_names) == set(PUBLIC_API_ROOTS)
    # every root contributes at least its own __init__.py
    assert "dlt/__init__.py" in source_files
    assert all(f.endswith(".py") for f in source_files)


def test_cmd_check_propagates_griffe_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """A griffe load failure raises instead of being reported as no breaking changes."""

    def _boom(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("invalid reference")

    monkeypatch.setattr(mod, "public_api", lambda: ({}, {"dlt/__init__.py"}))
    monkeypatch.setattr(mod.griffe, "load_git", _boom)

    with pytest.raises(RuntimeError, match="invalid reference"):
        mod.cmd_check("no-such-ref")


def test_detect_base_ref(monkeypatch: pytest.MonkeyPatch) -> None:
    # explicit arg takes priority over everything
    monkeypatch.setenv("GITHUB_BASE_REF", "main")
    assert detect_base_ref("my-branch") == "my-branch"

    # GITHUB_BASE_REF used when no explicit arg
    monkeypatch.setenv("GITHUB_BASE_REF", "main")
    assert detect_base_ref() == "main"
    assert detect_base_ref(None) == "main"

    # empty GITHUB_BASE_REF falls back to "devel"
    monkeypatch.setenv("GITHUB_BASE_REF", "")
    assert detect_base_ref() == "devel"

    # missing GITHUB_BASE_REF falls back to "devel"
    monkeypatch.delenv("GITHUB_BASE_REF", raising=False)
    assert detect_base_ref() == "devel"
