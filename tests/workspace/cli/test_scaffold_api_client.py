import io
import re
import shutil
import zipfile
from typing import Dict

import pytest
import requests
import requests_mock as rm

from dlt._workspace.cli._scaffold_api_client import (
    get_scaffold_files_storage,
    search_scaffolds,
    ScaffoldSearchResponse,
    ScaffoldSearchResult,
)
from dlt._workspace.cli.exceptions import ScaffoldSourceNotFound, ScaffoldApiError
from dlt.common.storages.file_storage import FileStorage

# pure unit tests for the client, integration tests with actual sources are in test_init_command.py


def _create_zip_response(files: Dict[str, str]) -> bytes:
    """Helper to create a ZIP archive from a dict of files."""
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for filename, content in files.items():
            zip_file.writestr(filename, content)
    zip_buffer.seek(0)
    return zip_buffer.getvalue()


def test_get_scaffold_files_storage_success(requests_mock: rm.Mocker) -> None:
    """Test successful retrieval of scaffold files from ZIP response with Accept header"""

    source_name = "github"
    mock_files = {
        "github.md": "# GitHub Source\nDocumentation content",
        "github-docs.yaml": "some more docs",
    }
    # Match URL without query params and verify Accept header
    requests_mock.get(
        re.compile(rf".*/api/v1/scaffolds/{source_name}/files$"),
        content=_create_zip_response(mock_files),
        headers={"Content-Type": "application/zip"},
    )

    storage = get_scaffold_files_storage(
        source_name, scaffold_docs_api_url="https://api.example.com"
    )

    assert isinstance(storage, FileStorage)
    # Verify files exist using has_file
    assert storage.has_file("github.md")
    assert storage.has_file("github-docs.yaml")
    # Verify file content using load
    assert storage.load("github.md") == "# GitHub Source\nDocumentation content"
    assert storage.load("github-docs.yaml") == "some more docs"

    # Verify that Accept header was sent
    assert requests_mock.last_request.headers.get("Accept") == "application/zip"

    # Cleanup temp directory
    shutil.rmtree(storage.storage_path, ignore_errors=True)


def test_get_scaffold_files_storage_not_found(requests_mock: rm.Mocker) -> None:
    """Test non-existing source raises ScaffoldSourceNotFound"""

    source_name = "non_existing_source"
    requests_mock.get(
        re.compile(rf".*/api/v1/scaffolds/{source_name}/files$"),
        status_code=404,
    )

    with pytest.raises(ScaffoldSourceNotFound) as exc_info:
        get_scaffold_files_storage(source_name, scaffold_docs_api_url="https://api.example.com")

    assert exc_info.value.source_name == source_name
    assert source_name in str(exc_info.value)
    # Verify that Accept header was sent
    assert requests_mock.last_request.headers.get("Accept") == "application/zip"


def test_get_scaffold_files_storage_invalid_zip(requests_mock: rm.Mocker) -> None:
    """Test invalid ZIP response raises ScaffoldApiError"""

    source_name = "some_source"
    requests_mock.get(
        re.compile(rf".*/api/v1/scaffolds/{source_name}/files$"),
        content=b"Not valid ZIP data{{{",
        status_code=200,
    )

    with pytest.raises(ScaffoldApiError) as exc_info:
        get_scaffold_files_storage(source_name, scaffold_docs_api_url="https://api.example.com")

    assert exc_info.value.source_name == source_name
    assert "Invalid ZIP response" in str(exc_info.value)
    # Verify that Accept header was sent
    assert requests_mock.last_request.headers.get("Accept") == "application/zip"


def test_get_scaffold_files_storage_server_error(requests_mock: rm.Mocker) -> None:
    """Test server error raises ScaffoldApiError"""

    source_name = "some_source"
    requests_mock.get(
        re.compile(rf".*/api/v1/scaffolds/{source_name}/files$"),
        status_code=500,
        text="Internal Server Error",
    )

    with pytest.raises(ScaffoldApiError) as exc_info:
        get_scaffold_files_storage(source_name, scaffold_docs_api_url="https://api.example.com")

    assert exc_info.value.source_name == source_name
    assert "500" in str(exc_info.value)
    # Verify that Accept header was sent
    assert requests_mock.last_request.headers.get("Accept") == "application/zip"


def test_search_scaffolds_with_keyword(requests_mock: rm.Mocker) -> None:
    mock_response = {
        "results": [
            {
                "content_path": "shopify_admin_rest",
                "name": "Shopify",
                "description": "Shopify e-commerce data",
            },
            {
                "content_path": "shopify_graphql",
                "name": "Shopify GraphQL",
                "description": "Shopify GraphQL API",
            },
        ],
        "total": 2,
    }
    requests_mock.get(
        re.compile(r".*/api/v1/scaffolds/sources"),
        json=mock_response,
    )

    response = search_scaffolds(
        search_term="shopif", scaffold_docs_api_url="https://api.example.com"
    )

    assert isinstance(response, ScaffoldSearchResponse)
    assert len(response.results) == 2
    assert response.total == 2
    assert response.results[0] == ScaffoldSearchResult(
        "shopify_admin_rest", "Shopify", "Shopify e-commerce data"
    )
    assert response.results[1] == ScaffoldSearchResult(
        "shopify_graphql", "Shopify GraphQL", "Shopify GraphQL API"
    )
    assert "q=shopif" in requests_mock.last_request.url


def test_search_scaffolds_empty_term(requests_mock: rm.Mocker) -> None:
    mock_response = {
        "results": [
            {"content_path": "airtable", "name": "Airtable", "description": "Airtable data"}
        ],
        "total": 9347,
    }
    requests_mock.get(
        re.compile(r".*/api/v1/scaffolds/sources"),
        json=mock_response,
    )

    response = search_scaffolds(scaffold_docs_api_url="https://api.example.com")

    assert response.total == 9347
    assert len(response.results) == 1
    assert "q=" in requests_mock.last_request.url


def test_search_scaffolds_connection_error(requests_mock: rm.Mocker) -> None:
    requests_mock.get(
        re.compile(r".*/api/v1/scaffolds/sources"),
        exc=requests.ConnectionError("Connection refused"),
    )

    with pytest.raises(ScaffoldApiError) as exc_info:
        search_scaffolds(scaffold_docs_api_url="https://api.example.com")

    assert "error connecting" in str(exc_info.value).lower()


def test_search_scaffolds_server_error(requests_mock: rm.Mocker) -> None:
    requests_mock.get(
        re.compile(r".*/api/v1/scaffolds/sources"),
        status_code=500,
        text="Internal Server Error",
    )

    with pytest.raises(ScaffoldApiError) as exc_info:
        search_scaffolds(scaffold_docs_api_url="https://api.example.com")

    assert "500" in str(exc_info.value)


def test_search_scaffolds_invalid_json(requests_mock: rm.Mocker) -> None:
    requests_mock.get(
        re.compile(r".*/api/v1/scaffolds/sources"),
        text="not valid json{{{",
        status_code=200,
    )

    with pytest.raises(ScaffoldApiError) as exc_info:
        search_scaffolds(scaffold_docs_api_url="https://api.example.com")

    assert "Invalid JSON" in str(exc_info.value)


def test_search_scaffolds_timeout(requests_mock: rm.Mocker) -> None:
    requests_mock.get(
        re.compile(r".*/api/v1/scaffolds/sources"),
        exc=requests.ReadTimeout("Read timed out"),
    )

    with pytest.raises(ScaffoldApiError) as exc_info:
        search_scaffolds(scaffold_docs_api_url="https://api.example.com")

    assert "error connecting" in str(exc_info.value).lower()
