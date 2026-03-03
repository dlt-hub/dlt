import io
import zipfile
import shutil

import pytest
import re
from typing import Dict
import requests_mock as rm

from dlt._workspace.cli._ai_context_api_client import get_ai_context_files_storage, search_sources
from dlt._workspace.cli.exceptions import ScaffoldSourceNotFound, ScaffoldApiError
from dlt.common.storages.file_storage import FileStorage


def _create_zip_response(files: Dict[str, str]) -> bytes:
    """Helper to create a ZIP archive from a dict of files."""
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for filename, content in files.items():
            zip_file.writestr(filename, content)
    zip_buffer.seek(0)
    return zip_buffer.getvalue()


def test_search_sources_with_query(requests_mock: rm.Mocker) -> None:
    """Test search_sources passes query param and returns results"""
    mock_response = {
        "results": [
            {"source_name": "github", "description": "GitHub API source"},
            {"source_name": "gitlab", "description": "GitLab API source"},
        ],
        "total": 2,
    }
    requests_mock.get(
        re.compile(r".*/api/v1/scaffolds/sources"),
        json=mock_response,
    )

    results = search_sources(query="git", ai_context_api_url="https://api.example.com")

    assert len(results) == 2
    assert results[0]["source_name"] == "github"
    assert results[1]["source_name"] == "gitlab"
    assert "q=git" in requests_mock.last_request.url


def test_search_sources_empty_query(requests_mock: rm.Mocker) -> None:
    """Test search_sources without query returns all sources"""
    mock_response = {
        "results": [{"source_name": "alpaca", "description": "Alpaca API"}],
        "total": 1,
    }
    requests_mock.get(
        re.compile(r".*/api/v1/scaffolds/sources"),
        json=mock_response,
    )

    results = search_sources(query="", ai_context_api_url="https://api.example.com")

    assert len(results) == 1
    assert "q=" not in requests_mock.last_request.url


def test_search_sources_server_error(requests_mock: rm.Mocker) -> None:
    """Test search_sources raises ScaffoldApiError on server error"""
    requests_mock.get(
        re.compile(r".*/api/v1/scaffolds/sources"),
        status_code=500,
        text="Internal Server Error",
    )

    with pytest.raises(ScaffoldApiError) as exc_info:
        search_sources(query="test", ai_context_api_url="https://api.example.com")

    assert "500" in str(exc_info.value)


def test_get_ai_context_files_storage_success(requests_mock: rm.Mocker) -> None:
    """Test successful retrieval of AI context files from ZIP response with Accept header"""

    source_name = "github"
    mock_files = {
        "github-docs.yaml": "some more docs",
    }
    requests_mock.get(
        re.compile(rf".*/api/v1/scaffolds/{source_name}/files$"),
        content=_create_zip_response(mock_files),
        headers={"Content-Type": "application/zip"},
    )

    storage = get_ai_context_files_storage(
        source_name, ai_context_api_url="https://api.example.com"
    )

    assert isinstance(storage, FileStorage)
    assert storage.has_file("github-docs.yaml")
    assert storage.load("github-docs.yaml") == "some more docs"

    assert requests_mock.last_request.headers.get("Accept") == "application/zip"

    shutil.rmtree(storage.storage_path, ignore_errors=True)


def test_get_ai_context_files_storage_not_found(requests_mock: rm.Mocker) -> None:
    """Test non-existing source raises ScaffoldSourceNotFound"""

    source_name = "non_existing_source"
    requests_mock.get(
        re.compile(rf".*/api/v1/scaffolds/{source_name}/files$"),
        status_code=404,
    )

    with pytest.raises(ScaffoldSourceNotFound) as exc_info:
        get_ai_context_files_storage(source_name, ai_context_api_url="https://api.example.com")

    assert exc_info.value.source_name == source_name
    assert source_name in str(exc_info.value)
    assert requests_mock.last_request.headers.get("Accept") == "application/zip"


def test_get_ai_context_files_storage_invalid_zip(requests_mock: rm.Mocker) -> None:
    """Test invalid ZIP response raises ScaffoldApiError"""

    source_name = "some_source"
    requests_mock.get(
        re.compile(rf".*/api/v1/scaffolds/{source_name}/files$"),
        content=b"Not valid ZIP data{{{",
        status_code=200,
    )

    with pytest.raises(ScaffoldApiError) as exc_info:
        get_ai_context_files_storage(source_name, ai_context_api_url="https://api.example.com")

    assert exc_info.value.source_name == source_name
    assert "Invalid ZIP response" in str(exc_info.value)
    assert requests_mock.last_request.headers.get("Accept") == "application/zip"


def test_get_ai_context_files_storage_server_error(requests_mock: rm.Mocker) -> None:
    """Test server error raises ScaffoldApiError"""

    source_name = "some_source"
    requests_mock.get(
        re.compile(rf".*/api/v1/scaffolds/{source_name}/files$"),
        status_code=500,
        text="Internal Server Error",
    )

    with pytest.raises(ScaffoldApiError) as exc_info:
        get_ai_context_files_storage(source_name, ai_context_api_url="https://api.example.com")

    assert exc_info.value.source_name == source_name
    assert "500" in str(exc_info.value)
    assert requests_mock.last_request.headers.get("Accept") == "application/zip"
