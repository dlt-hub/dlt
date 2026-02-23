"""Tests for tools/check_api_breaking.py."""

import pytest

from tools.check_api_breaking import classify_griffe_output
from tools.git_utils import detect_base_ref

SOURCE_FILES = {
    "dlt/__init__.py",
    "dlt/pipeline/pipeline.py",
    "dlt/extract/resource.py",
    "dlt/sources/__init__.py",
    "dlt/common/destination/dataset.py",
}

PUBLIC_LINES = [
    "dlt/__init__.py:42: Parameter 'destination' was removed from function 'pipeline'",
    "dlt/pipeline/pipeline.py:630: Method 'run' was removed from class 'Pipeline'",
    "dlt/extract/resource.py:100: Attribute 'name' was removed",
    "dlt/sources/__init__.py:10: Function 'resource' was removed",
    "dlt/common/destination/dataset.py:5: Class 'Dataset' was removed",
]

NON_PUBLIC_LINES = [
    "dlt/normalize/items_normalizers.py:15: Function '_normalize' was removed",
    "dlt/common/storages/file_storage.py:80: Parameter 'encoding' was removed",
    "dlt/destinations/impl/duckdb/duck.py:22: Method '_execute' was removed",
]


def test_skipped_lines() -> None:
    """Empty, whitespace-only, and warning lines are all dropped."""
    for output in ["", "  \n\n  \n", "warning: could not find module foo\n"]:
        kept, pruned = classify_griffe_output(output, SOURCE_FILES)
        assert kept == []
        assert pruned == []


@pytest.mark.parametrize(
    "line",
    PUBLIC_LINES,
    ids=["init", "pipeline", "resource", "sources_init", "dataset"],
)
def test_public_module_line_kept(line: str) -> None:
    kept, pruned = classify_griffe_output(line, SOURCE_FILES)
    assert kept == [line]
    assert pruned == []


@pytest.mark.parametrize(
    "line",
    NON_PUBLIC_LINES,
    ids=["normalize", "storages", "duckdb_impl"],
)
def test_non_public_module_line_pruned(line: str) -> None:
    kept, pruned = classify_griffe_output(line, SOURCE_FILES)
    assert kept == []
    assert pruned == [line]


def test_mixed_output() -> None:
    """Public, non-public, warnings, and blanks are classified correctly."""
    output = (
        "warning: could not read dlt/foo.py\n\n"
        + "\n".join(PUBLIC_LINES)
        + "\n  \n"
        + "\n".join(NON_PUBLIC_LINES)
        + "\nwarning: done\n"
    )
    kept, pruned = classify_griffe_output(output, SOURCE_FILES)
    assert len(kept) == len(PUBLIC_LINES)
    assert len(pruned) == len(NON_PUBLIC_LINES)


def test_edge_cases() -> None:
    # leading whitespace is stripped before matching
    kept, _ = classify_griffe_output("   dlt/__init__.py:1: Something changed\n", SOURCE_FILES)
    assert len(kept) == 1

    # empty source_files set prunes everything
    kept, pruned = classify_griffe_output(PUBLIC_LINES[0], set())
    assert kept == []
    assert len(pruned) == 1

    # resource.py should not match resource_helpers.py
    kept, pruned = classify_griffe_output(
        "dlt/extract/resource_helpers.py:10: Function removed\n",
        SOURCE_FILES,
    )
    assert kept == []
    assert len(pruned) == 1


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
