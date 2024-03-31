from unittest.mock import Mock

import pytest

from requests.models import Response

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


class TestSinglePagePaginator:
    def test_update_state(self):
        paginator = SinglePagePaginator()
        response = Mock(Response)
        paginator.update_state(response)
        assert paginator.has_next_page is False

    def test_update_state_with_next(self):
        paginator = SinglePagePaginator()
        response = Mock(
            Response, json=lambda: {"next": "http://example.com/next", "results": []}
        )
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

    def test_update_state_without_total(self):
        paginator = OffsetPaginator(0, 10)
        response = Mock(Response, json=lambda: {})
        with pytest.raises(ValueError):
            paginator.update_state(response)
