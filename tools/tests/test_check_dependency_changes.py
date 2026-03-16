"""Tests for tools/check_dependency_changes.py.

Uses static test case files from tools/tests/cases/dependency_changes/
instead of relying on git history, so tests work reliably in CI.
"""

import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from unittest.mock import patch

import pytest
import tomlkit

from tools.check_dependency_changes import (
    diff_deps,
    main,
    normalize_name,
    parse_dep_list,
    three_way_merge,
)

# Path to test case fixtures
CASES_DIR = Path(__file__).parent / "cases" / "dependency_changes"


def load_test_case(case_name: str) -> Dict[str, str]:
    """Load ancestor, base, and head pyproject.toml for a test case."""
    case_dir = CASES_DIR / case_name
    return {
        "ancestor": (case_dir / "ancestor.toml").read_text(),
        "base": (case_dir / "base.toml").read_text(),
        "head": (case_dir / "head.toml").read_text(),
    }


def mock_git_for_case(
    case_name: str,
) -> Tuple[Callable[[str, str], Optional[str]], Callable[[str, str], Optional[str]]]:
    """Return mock functions for git_show and git_merge_base using test case files."""
    case_data = load_test_case(case_name)

    def mock_git_show(ref: str, path: str = "pyproject.toml") -> Optional[str]:
        if ref == "mock_ancestor":
            return case_data["ancestor"]
        elif ref == "mock_base":
            return case_data["base"]
        elif ref == "mock_head":
            return case_data["head"]
        return None

    def mock_git_merge_base(ref1: str, ref2: str) -> Optional[str]:
        return "mock_ancestor"

    return mock_git_show, mock_git_merge_base


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("PyYAML", "pyyaml"),
        ("my-package", "my_package"),
        ("zope.interface", "zope_interface"),
        ("My-Cool.Package", "my_cool_package"),
    ],
    ids=["lowercase", "hyphens", "dots", "combined"],
)
def test_normalize_name(raw: str, expected: str) -> None:
    assert normalize_name(raw) == expected


def test_parse_dep_list() -> None:
    # empty
    assert parse_dep_list([]) == {}

    # simple parsing and name normalization
    result = parse_dep_list(["requests>=2.26.0", "PyYAML>=5.4.1"])
    assert result["requests"] == ["requests>=2.26.0"]
    assert "pyyaml" in result

    # multiple entries with different markers
    deps = [
        "pendulum>=2.1.2",
        "pendulum>=3.0.0 ; python_version > '3.13'",
    ]
    assert len(parse_dep_list(deps)["pendulum"]) == 2

    # PEP 735 include-group dicts are skipped
    mixed_deps: List[Any] = ["requests>=2.0", {"include-group": "other"}, "click>=7.1"]
    assert len(parse_dep_list(mixed_deps)) == 2


def test_diff_deps() -> None:
    # no changes
    deps = {"requests": ["requests>=2.26.0"]}
    assert diff_deps(deps, deps) == []

    # added, removed, changed in one diff
    old = {"a": ["a>=1"], "b": ["b>=1"]}
    new = {"b": ["b>=2"], "c": ["c>=1"]}
    changes = diff_deps(old, new)
    names = {c.name: c.kind for c in changes}
    assert names == {"a": "removed", "b": "changed", "c": "added"}


def test_three_way_merge() -> None:
    ancestor = "line1\nline2\nline3\n"

    # clean merge
    merged, conflicts = three_way_merge(ancestor, ancestor, "line1\nLINE2\nline3\n")
    assert not conflicts
    assert "LINE2" in merged

    # conflict
    merged, conflicts = three_way_merge("line1\nOURS\nline3\n", ancestor, "line1\nTHEIRS\nline3\n")
    assert conflicts
    assert "<<<<<<" in merged


def test_no_dep_changes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test case where PR does not touch pyproject.toml (ancestor == head)."""
    mock_show, mock_merge_base = mock_git_for_case("no_pyproject_change")

    with patch("tools.check_dependency_changes.git_show", mock_show):
        with patch("tools.check_dependency_changes.git_merge_base", mock_merge_base):
            monkeypatch.setattr(sys, "argv", ["prog", "mock_base", "mock_head"])
            assert main() == 0


def test_sqlglot_revert(monkeypatch: pytest.MonkeyPatch) -> None:
    """Commit ccec5c0f8 reverts sqlglot constraints — main dep change only."""
    case_data = load_test_case("sqlglot_revert")
    base_doc = dict(tomlkit.parse(case_data["base"]))
    head_doc = dict(tomlkit.parse(case_data["head"]))

    # main deps: sqlglot specifier changed
    base_main = parse_dep_list(list(base_doc["project"]["dependencies"]))  # type: ignore[index,arg-type]
    head_main = parse_dep_list(list(head_doc["project"]["dependencies"]))  # type: ignore[index,arg-type]
    changes = diff_deps(base_main, head_main)
    sqlglot = [c for c in changes if c.name == "sqlglot"]
    assert len(sqlglot) == 1
    assert sqlglot[0].kind == "changed"

    # optional extras should be untouched
    base_opt = base_doc["project"]["optional-dependencies"]  # type: ignore[index]
    head_opt = head_doc["project"]["optional-dependencies"]  # type: ignore[index]
    for extra in set(base_opt) | set(head_opt):  # type: ignore[arg-type]
        changes = diff_deps(
            parse_dep_list(list(base_opt.get(extra, []))),  # type: ignore[union-attr]
            parse_dep_list(list(head_opt.get(extra, []))),  # type: ignore[union-attr]
        )
        assert changes == [], f"unexpected optional dep change in [{extra}]"

    mock_show, mock_merge_base = mock_git_for_case("sqlglot_revert")
    with patch("tools.check_dependency_changes.git_show", mock_show):
        with patch("tools.check_dependency_changes.git_merge_base", mock_merge_base):
            monkeypatch.setattr(sys, "argv", ["prog", "mock_base", "mock_head"])
            assert main() == 1


def test_multi_section_changes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Commit 215888991 changes main deps (sqlglot) and dev group (pytest-xdist)."""
    case_data = load_test_case("faster_testing")
    base_doc = dict(tomlkit.parse(case_data["base"]))
    head_doc = dict(tomlkit.parse(case_data["head"]))

    # main deps: sqlglot version specifier changed
    base_main = parse_dep_list(list(base_doc["project"]["dependencies"]))  # type: ignore[index,arg-type]
    head_main = parse_dep_list(list(head_doc["project"]["dependencies"]))  # type: ignore[index,arg-type]
    assert any(c.name == "sqlglot" for c in diff_deps(base_main, head_main))

    # dev group: pytest-xdist was added
    base_dev = parse_dep_list(list(base_doc["dependency-groups"]["dev"]))  # type: ignore[index,arg-type]
    head_dev = parse_dep_list(list(head_doc["dependency-groups"]["dev"]))  # type: ignore[index,arg-type]
    xdist = [c for c in diff_deps(base_dev, head_dev) if c.name == "pytest_xdist"]
    assert len(xdist) == 1
    assert xdist[0].kind == "added"

    mock_show, mock_merge_base = mock_git_for_case("faster_testing")
    with patch("tools.check_dependency_changes.git_show", mock_show):
        with patch("tools.check_dependency_changes.git_merge_base", mock_merge_base):
            monkeypatch.setattr(sys, "argv", ["prog", "mock_base", "mock_head"])
            assert main() == 1
