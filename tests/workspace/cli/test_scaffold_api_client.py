import io
import zipfile
import shutil

import pytest
import re
from typing import Dict
import requests_mock as rm

from dlt._workspace.cli._scaffold_api_client import get_scaffold_files_storage
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
    # Verify files exist using has_file (scaffold API returns only yaml, no md)
    assert storage.has_file("github-docs.yaml")
    # Verify file content using load
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
