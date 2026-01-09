import pytest
import re
import requests_mock as rm

from dlt._workspace.cli._scaffold_api_client import _get_scaffold_files
from dlt._workspace.cli.exceptions import ScaffoldSourceNotFound, ScaffoldApiError

# pure unit tests for the client, integration tests with actual sources are in test_init_command.py


def test_get_scaffold_files_success(requests_mock: rm.Mocker) -> None:
    """Test successful retrieval of scaffold files for existing source"""

    source_name = "github"
    mock_response = {
        "files": {
            "github.md": "# GitHub Source\nDocumentation content",
            "github-docs.yaml": "some more docs",
        }
    }
    requests_mock.get(
        re.compile(rf".*/api/v1/scaffolds/{source_name}/files$"),
        json=mock_response,
    )

    result = _get_scaffold_files(source_name)

    assert isinstance(result, dict)
    assert len(result) == 2
    for filename, content in result.items():
        assert isinstance(filename, str)
        assert isinstance(content, str)
    assert result["github.md"] is not None
    assert result["github-docs.yaml"] is not None


def test_get_scaffold_files_not_found(requests_mock: rm.Mocker) -> None:
    """Test non-existing source raises ScaffoldSourceNotFound"""

    source_name = "non_existing_source"
    requests_mock.get(
        re.compile(rf".*/api/v1/scaffolds/{source_name}/files$"),
        status_code=404,
    )

    with pytest.raises(ScaffoldSourceNotFound) as exc_info:
        _get_scaffold_files(source_name)

    assert exc_info.value.source_name == source_name
    assert source_name in str(exc_info.value)


def test_get_scaffold_files_invalid_json(requests_mock: rm.Mocker) -> None:
    """Test invalid JSON response raises ScaffoldApiError"""

    source_name = "some_source"
    requests_mock.get(
        re.compile(rf".*/api/v1/scaffolds/{source_name}/files$"),
        text="Not valid JSON{{{",
        status_code=200,
    )

    with pytest.raises(ScaffoldApiError) as exc_info:
        _get_scaffold_files(source_name)

    assert exc_info.value.source_name == source_name
    assert "Invalid JSON response" in str(exc_info.value)


def test_get_scaffold_files_missing_files_key(requests_mock: rm.Mocker) -> None:
    """Test response missing 'files' key raises ScaffoldApiError"""

    source_name = "some_source"
    requests_mock.get(
        re.compile(rf".*/api/v1/scaffolds/{source_name}/files$"),
        json={"data": {}, "status": "ok"},  # Wrong structure
        status_code=200,
    )

    with pytest.raises(ScaffoldApiError) as exc_info:
        _get_scaffold_files(source_name)

    assert exc_info.value.source_name == source_name
    assert "expected dict with 'files' key" in str(exc_info.value)


def test_get_scaffold_files_files_not_dict(requests_mock: rm.Mocker) -> None:
    """Test response with 'files' as non-dict raises ScaffoldApiError"""

    source_name = "some_source"
    requests_mock.get(
        re.compile(rf".*/api/v1/scaffolds/{source_name}/files$"),
        json={"files": ["file1.md", "file2.md"]},  # files is a list, not dict
        status_code=200,
    )

    with pytest.raises(ScaffoldApiError) as exc_info:
        _get_scaffold_files(source_name)

    assert exc_info.value.source_name == source_name
    assert "'files' must be a dict" in str(exc_info.value)


def test_get_scaffold_files_response_not_dict(requests_mock: rm.Mocker) -> None:
    """Test response that's not a dict raises ScaffoldApiError"""

    source_name = "some_source"
    requests_mock.get(
        re.compile(rf".*/api/v1/scaffolds/{source_name}/files$"),
        json=["item1", "item2"],  # Response is a list, not dict
        status_code=200,
    )

    with pytest.raises(ScaffoldApiError) as exc_info:
        _get_scaffold_files(source_name)

    assert exc_info.value.source_name == source_name
    assert "expected dict with 'files' key" in str(exc_info.value)
