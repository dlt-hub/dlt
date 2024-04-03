from unittest.mock import Mock

import pytest

from requests.models import Response, Request

from dlt.sources.helpers.rest_client.paginators import (
    SinglePagePaginator,
    OffsetPaginator,
    HeaderLinkPaginator,
    JSONResponsePaginator,
)


class TestHeaderLinkPaginator:
    def test_update_state_with_next(self):
        paginator = HeaderLinkPaginator()
        response = Mock(Response)
        response.links = {"next": {"url": "http://example.com/next"}}
        paginator.update_state(response)
        assert paginator.next_reference == "http://example.com/next"
        assert paginator.has_next_page is True

    def test_update_state_without_next(self):
        paginator = HeaderLinkPaginator()
        response = Mock(Response)
        response.links = {}
        paginator.update_state(response)
        assert paginator.has_next_page is False


class TestJSONResponsePaginator:
    @pytest.mark.parametrize(
        "test_case",
        [
            # Test with empty next_url_path, e.g. auto-detect
            {
                "next_url_path": None,
                "response_json": {"next": "http://example.com/next", "results": []},
                "expected": {
                    "next_reference": "http://example.com/next",
                    "has_next_page": True,
                },
            },
            # Test with explicit next_url_path
            {
                "next_url_path": "next_page",
                "response_json": {
                    "next_page": "http://example.com/next",
                    "results": [],
                },
                "expected": {
                    "next_reference": "http://example.com/next",
                    "has_next_page": True,
                },
            },
            # Test with nested next_url_path
            {
                "next_url_path": "next_page.url",
                "response_json": {
                    "next_page": {"url": "http://example.com/next"},
                    "results": [],
                },
                "expected": {
                    "next_reference": "http://example.com/next",
                    "has_next_page": True,
                },
            },
            # Test without next_page
            {
                "next_url_path": None,
                "response_json": {"results": []},
                "expected": {
                    "next_reference": None,
                    "has_next_page": False,
                },
            },
        ],
    )
    def test_update_state(self, test_case):
        next_url_path = test_case["next_url_path"]

        if next_url_path is None:
            paginator = JSONResponsePaginator()
        else:
            paginator = JSONResponsePaginator(next_url_path=next_url_path)
        response = Mock(Response, json=lambda: test_case["response_json"])
        paginator.update_state(response)
        assert paginator.next_reference == test_case["expected"]["next_reference"]
        assert paginator.has_next_page == test_case["expected"]["has_next_page"]

    # Test update_request from BaseNextUrlPaginator
    @pytest.mark.parametrize(
        "test_case",
        [
            # Test with absolute URL
            {
                "next_reference": "http://example.com/api/resource?page=2",
                "request_url": "http://example.com/api/resource",
                "expected": "http://example.com/api/resource?page=2",
            },
            # Test with relative URL
            {
                "next_reference": "/api/resource?page=2",
                "request_url": "http://example.com/api/resource",
                "expected": "http://example.com/api/resource?page=2",
            },
            # Test with more nested path
            {
                "next_reference": "/api/resource/subresource?page=3&sort=desc",
                "request_url": "http://example.com/api/resource/subresource",
                "expected": "http://example.com/api/resource/subresource?page=3&sort=desc",
            },
            # Test with 'page' in path
            {
                "next_reference": "/api/page/4/items?filter=active",
                "request_url": "http://example.com/api/page/3/items",
                "expected": "http://example.com/api/page/4/items?filter=active",
            },
            # Test with complex query parameters
            {
                "next_reference": "/api/resource?page=3&category=books&sort=author",
                "request_url": "http://example.com/api/resource?page=2",
                "expected": "http://example.com/api/resource?page=3&category=books&sort=author",
            },
            # Test with URL having port number
            {
                "next_reference": "/api/resource?page=2",
                "request_url": "http://example.com:8080/api/resource",
                "expected": "http://example.com:8080/api/resource?page=2",
            },
            # Test with HTTPS protocol
            {
                "next_reference": "https://secure.example.com/api/resource?page=2",
                "request_url": "https://secure.example.com/api/resource",
                "expected": "https://secure.example.com/api/resource?page=2",
            },
            # Test with encoded characters in URL
            {
                "next_reference": "/api/resource?page=2&query=%E3%81%82",
                "request_url": "http://example.com/api/resource",
                "expected": "http://example.com/api/resource?page=2&query=%E3%81%82",
            },
            # Test with missing 'page' parameter in next_reference
            {
                "next_reference": "/api/resource?sort=asc",
                "request_url": "http://example.com/api/resource?page=1",
                "expected": "http://example.com/api/resource?sort=asc",
            },
        ],
    )
    def test_update_request(self, test_case):
        paginator = JSONResponsePaginator()
        paginator.next_reference = test_case["next_reference"]
        request = Mock(Request, url=test_case["request_url"])
        paginator.update_request(request)
        assert request.url == test_case["expected"]


class TestSinglePagePaginator:
    def test_update_state(self):
        paginator = SinglePagePaginator()
        response = Mock(Response)
        paginator.update_state(response)
        assert paginator.has_next_page is False

    def test_update_state_with_next(self):
        paginator = SinglePagePaginator()
        response = Mock(Response, json=lambda: {"next": "http://example.com/next", "results": []})
        response.links = {"next": {"url": "http://example.com/next"}}
        paginator.update_state(response)
        assert paginator.has_next_page is False


class TestOffsetPaginator:
    def test_update_state(self):
        paginator = OffsetPaginator(initial_offset=0, initial_limit=10)
        response = Mock(Response, json=lambda: {"total": 20})
        paginator.update_state(response)
        assert paginator.offset == 10
        assert paginator.has_next_page is True

        # Test for reaching the end
        paginator.update_state(response)
        assert paginator.has_next_page is False

    def test_update_state_with_string_total(self):
        paginator = OffsetPaginator(0, 10)
        response = Mock(Response, json=lambda: {"total": "20"})
        paginator.update_state(response)
        assert paginator.offset == 10
        assert paginator.has_next_page is True

    def test_update_state_with_invalid_total(self):
        paginator = OffsetPaginator(0, 10)
        response = Mock(Response, json=lambda: {"total": "invalid"})
        with pytest.raises(ValueError):
            paginator.update_state(response)

    def test_update_state_without_total(self):
        paginator = OffsetPaginator(0, 10)
        response = Mock(Response, json=lambda: {})
        with pytest.raises(ValueError):
            paginator.update_state(response)
