import pytest
from unittest.mock import Mock

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
        "next_url_path, response_json, expected_next_reference, expected_has_next_page",
        [
            # Test with empty next_url_path, e.g. auto-detect
            (
                None,
                {"next": "http://example.com/next", "results": []},
                "http://example.com/next",
                True,
            ),
            # Test with explicit next_url_path
            (
                "next_page",
                {"next_page": "http://example.com/next", "results": []},
                "http://example.com/next",
                True,
            ),
            # Test with nested next_url_path
            (
                "next_page.url",
                {"next_page": {"url": "http://example.com/next"}, "results": []},
                "http://example.com/next",
                True,
            ),
            # Test without next_page
            (
                None,
                {"results": []},
                None,
                False,
            ),
        ],
    )
    def test_update_state(
        self,
        next_url_path,
        response_json,
        expected_next_reference,
        expected_has_next_page,
    ):
        if next_url_path is None:
            paginator = JSONResponsePaginator()
        else:
            paginator = JSONResponsePaginator(next_url_path=next_url_path)
        response = Mock(Response, json=lambda: response_json)
        paginator.update_state(response)
        assert paginator.next_reference == expected_next_reference
        assert paginator.has_next_page == expected_has_next_page


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
