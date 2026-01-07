import pytest

from dlt._workspace.cli._scaffold_api_client import _get_scaffold_files
from dlt._workspace.cli.exceptions import ScaffoldSourceNotFound


def test_get_scaffold_files_success():
    """Test successful retrieval of scaffold files for existing source"""
    result = _get_scaffold_files("github")

    assert isinstance(result, dict)
    assert len(result) == 2
    for filename, content in result.items():
        assert isinstance(filename, str)
        assert isinstance(content, str)
    assert result["github.md"] is not None
    assert result["github-docs.yaml"] is not None


def test_get_scaffold_files_not_found():
    """Test non-existing source raises ScaffoldSourceNotFound"""
    with pytest.raises(ScaffoldSourceNotFound) as exc_info:
        _get_scaffold_files("non_existing_source")

    assert exc_info.value.source_name == "non_existing_source"
    assert "non_existing_source" in str(exc_info.value)
