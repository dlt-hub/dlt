import os
import pytest

from dlt._workspace.deployment.file_selector import WorkspaceFileSelector

from tests.workspace.utils import isolated_workspace, WORKSPACE_CASES_DIR


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

    run_dir = os.path.join(WORKSPACE_CASES_DIR, "default")
    with isolated_workspace(run_dir, "test_file_selector") as ctx:
        selector = WorkspaceFileSelector(
            ctx, additional_excludes=additional_excludes, ignore_file=".ignorefile"
        )
        files = set([f.as_posix() for f in selector])
        assert files == expected_files
