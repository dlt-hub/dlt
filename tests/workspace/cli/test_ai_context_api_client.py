import pytest
import re

import requests_mock as rm

from dlt._workspace.cli._ai_context_api_client import search_sources
from dlt._workspace.cli.exceptions import AiContextApiError


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
    """Test search_sources raises AiContextApiError on server error"""
    requests_mock.get(
        re.compile(r".*/api/v1/scaffolds/sources"),
        status_code=500,
        text="Internal Server Error",
    )

    with pytest.raises(AiContextApiError) as exc_info:
        search_sources(query="test", ai_context_api_url="https://api.example.com")

    assert "500" in str(exc_info.value)
