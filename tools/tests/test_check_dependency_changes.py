"""Tests for tools/check_dependency_changes.py.

Uses fixed commit hashes from repo history so tests remain stable.
Requires running inside a git clone of the dlt repository with full history.
"""

import sys
from typing import Any, Dict, List

import pytest
import tomlkit

from tools.check_dependency_changes import (
    diff_deps,
    main,
    normalize_name,
    parse_dep_list,
    three_way_merge,
)
from tools.git_utils import git_merge_base, git_show

# ccec5c0f8 "fix: revert sqlglot version constraints (#3575)"
# main dep change: sqlglot>=25.4.0,<28 -> sqlglot>=25.4.0,!=28.1
SQLGLOT_REVERT = "ccec5c0f80625a9b47fc8325690b82dfd50af94b"

# 215888991 "Feat/faster testing (#3479)" (parent of SQLGLOT_REVERT)
# main dep change: sqlglot>=25.4.0,!=28.1 -> sqlglot>=25.4.0,<28
# dev group change: added pytest-xdist>=3.5,<4
FASTER_TESTING = "215888991ffa4c8fdd2d5554308c77427aa15f61"
FASTER_TESTING_PARENT = "ca294e057e7234f99f87904b591d88eb8fb9f5ce"

# ea8b1ae7b "(fix) do not write `_dlt_load_id` as dict on mssql + adbc (#3584)"
# does NOT touch pyproject.toml at all
NO_PYPROJECT_CHANGE = "ea8b1ae7bb4d0e4b5f3eb5a6bef0e2559b1aa641"
NO_PYPROJECT_CHANGE_PARENT = "6979e42b66c8ba890bf77a67d93cd5a7fccdd4a4"


def parse_pyproject(ref: str) -> Dict[str, Any]:
    text = git_show(ref)
    assert text is not None
    return dict(tomlkit.parse(text))


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
    """Commit ea8b1ae7b does not touch pyproject.toml."""
    monkeypatch.setattr(sys, "argv", ["prog", NO_PYPROJECT_CHANGE_PARENT, NO_PYPROJECT_CHANGE])
    assert main() == 0

    # ancestor and head have identical pyproject.toml
    ancestor = git_merge_base(NO_PYPROJECT_CHANGE_PARENT, NO_PYPROJECT_CHANGE)
    assert ancestor is not None
    assert git_show(ancestor) == git_show(NO_PYPROJECT_CHANGE)


def test_sqlglot_revert(monkeypatch: pytest.MonkeyPatch) -> None:
    """Commit ccec5c0f8 reverts sqlglot constraints — main dep change only."""
    monkeypatch.setattr(sys, "argv", ["prog", FASTER_TESTING, SQLGLOT_REVERT])
    assert main() == 1

    base_doc = parse_pyproject(FASTER_TESTING)
    head_doc = parse_pyproject(SQLGLOT_REVERT)

    # main deps: sqlglot specifier changed
    base_main = parse_dep_list(list(base_doc["project"]["dependencies"]))
    head_main = parse_dep_list(list(head_doc["project"]["dependencies"]))
    changes = diff_deps(base_main, head_main)
    sqlglot = [c for c in changes if c.name == "sqlglot"]
    assert len(sqlglot) == 1
    assert sqlglot[0].kind == "changed"

    # optional extras should be untouched
    base_opt = base_doc["project"]["optional-dependencies"]
    head_opt = head_doc["project"]["optional-dependencies"]
    for extra in set(base_opt) | set(head_opt):
        changes = diff_deps(
            parse_dep_list(list(base_opt.get(extra, []))),
            parse_dep_list(list(head_opt.get(extra, []))),
        )
        assert changes == [], f"unexpected optional dep change in [{extra}]"


def test_multi_section_changes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Commit 215888991 changes main deps (sqlglot) and dev group (pytest-xdist)."""
    monkeypatch.setattr(sys, "argv", ["prog", FASTER_TESTING_PARENT, FASTER_TESTING])
    assert main() == 1

    base_doc = parse_pyproject(FASTER_TESTING_PARENT)
    head_doc = parse_pyproject(FASTER_TESTING)

    # main deps: sqlglot version specifier changed
    base_main = parse_dep_list(list(base_doc["project"]["dependencies"]))
    head_main = parse_dep_list(list(head_doc["project"]["dependencies"]))
    assert any(c.name == "sqlglot" for c in diff_deps(base_main, head_main))

    # dev group: pytest-xdist was added
    base_dev = parse_dep_list(list(base_doc["dependency-groups"]["dev"]))
    head_dev = parse_dep_list(list(head_doc["dependency-groups"]["dev"]))
    xdist = [c for c in diff_deps(base_dev, head_dev) if c.name == "pytest_xdist"]
    assert len(xdist) == 1
    assert xdist[0].kind == "added"
