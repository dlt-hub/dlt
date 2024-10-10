from typing import Any, List
from unittest.mock import Mock

import pytest

from requests.models import Response, Request
from requests import Session

from dlt.sources.helpers.rest_client.paginators import (
    SinglePagePaginator,
    OffsetPaginator,
    PageNumberPaginator,
    HeaderLinkPaginator,
    JSONLinkPaginator,
    JSONResponseCursorPaginator,
)

from .conftest import assert_pagination

NON_EMPTY_PAGE = [{"some": "data"}]


@pytest.mark.usefixtures("mock_api_server")
class TestHeaderLinkPaginator:
    def test_update_state_with_next(self):
        paginator = HeaderLinkPaginator()
        response = Mock(Response)
        response.links = {"next": {"url": "http://example.com/next"}}
        paginator.update_state(response)
        assert paginator._next_reference == "http://example.com/next"
        assert paginator.has_next_page is True

    def test_update_state_without_next(self):
        paginator = HeaderLinkPaginator()
        response = Mock(Response)
        response.links = {}
        paginator.update_state(response)
        assert paginator.has_next_page is False

    def test_client_pagination(self, rest_client):
        pages_iter = rest_client.paginate(
            "/posts_header_link",
            paginator=HeaderLinkPaginator(),
        )

        pages = list(pages_iter)

        assert_pagination(pages)


@pytest.mark.usefixtures("mock_api_server")
class TestJSONLinkPaginator:
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
            paginator = JSONLinkPaginator()
        else:
            paginator = JSONLinkPaginator(next_url_path=next_url_path)
        response = Mock(Response, json=lambda: test_case["response_json"])
        paginator.update_state(response)
        assert paginator._next_reference == test_case["expected"]["next_reference"]
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
        paginator = JSONLinkPaginator()
        paginator._next_reference = test_case["next_reference"]
        request = Mock(Request, url=test_case["request_url"])
        paginator.update_request(request)
        assert request.url == test_case["expected"]

    def test_no_duplicate_params_on_update_request(self):
        paginator = JSONLinkPaginator()

        request = Request(
            method="GET",
            url="http://example.com/api/resource",
            params={"param1": "value1"},
        )

        session = Session()

        response = Mock(Response, json=lambda: {"next": "/api/resource?page=2&param1=value1"})
        paginator.update_state(response)
        paginator.update_request(request)

        assert request.url == "http://example.com/api/resource?page=2&param1=value1"

        # RESTClient._send_request() calls Session.prepare_request() which
        # updates the URL with the query parameters from the request object.
        prepared_request = session.prepare_request(request)

        # The next request should just use the "next" URL without any duplicate parameters.
        assert prepared_request.url == "http://example.com/api/resource?page=2&param1=value1"

    def test_client_pagination(self, rest_client):
        pages_iter = rest_client.paginate(
            "/posts",
            paginator=JSONLinkPaginator(
                next_url_path="next_page",
            ),
        )

        pages = list(pages_iter)

        assert_pagination(pages)


@pytest.mark.usefixtures("mock_api_server")
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

    def test_client_pagination(self, rest_client):
        pages_iter = rest_client.paginate(
            "/posts",
            paginator=SinglePagePaginator(),
        )

        pages = list(pages_iter)

        assert_pagination(pages, total_pages=1)


@pytest.mark.usefixtures("mock_api_server")
class TestOffsetPaginator:
    def test_update_state(self):
        paginator = OffsetPaginator(offset=0, limit=10)
        response = Mock(Response, json=lambda: {"total": 20})
        paginator.update_state(response, data=NON_EMPTY_PAGE)
        assert paginator.current_value == 10
        assert paginator.has_next_page is True

        # Test for reaching the end
        paginator.update_state(response)
        assert paginator.has_next_page is False

    def test_update_state_with_string_total(self):
        paginator = OffsetPaginator(0, 10)
        response = Mock(Response, json=lambda: {"total": "20"})
        paginator.update_state(response, data=NON_EMPTY_PAGE)
        assert paginator.current_value == 10
        assert paginator.has_next_page is True

    def test_update_state_with_invalid_total(self):
        paginator = OffsetPaginator(0, 10)
        response = Mock(Response, json=lambda: {"total": "invalid"})
        with pytest.raises(ValueError):
            paginator.update_state(response, data=NON_EMPTY_PAGE)

    def test_update_state_without_total(self):
        paginator = OffsetPaginator(0, 10)
        response = Mock(Response, json=lambda: {})
        with pytest.raises(ValueError):
            paginator.update_state(response, data=NON_EMPTY_PAGE)

    def test_init_request(self):
        paginator = OffsetPaginator(offset=123, limit=42)
        request = Mock(Request)
        request.params = {}

        paginator.init_request(request)

        assert request.params["offset"] == 123
        assert request.params["limit"] == 42

        response = Mock(Response, json=lambda: {"total": 200})

        paginator.update_state(response, data=NON_EMPTY_PAGE)

        # Test for the next request
        next_request = Mock(spec=Request)
        next_request.params = {}

        paginator.update_request(next_request)

        assert next_request.params["offset"] == 165
        assert next_request.params["limit"] == 42

    def test_maximum_offset(self):
        paginator = OffsetPaginator(offset=0, limit=50, maximum_offset=100, total_path=None)
        response = Mock(Response, json=lambda: {"items": []})
        paginator.update_state(response, data=NON_EMPTY_PAGE)  # Offset 0 to 50
        assert paginator.current_value == 50
        assert paginator.has_next_page is True

        paginator.update_state(response, data=NON_EMPTY_PAGE)  # Offset 50 to 100
        assert paginator.current_value == 100
        assert paginator.has_next_page is False

    def test_client_pagination(self, rest_client):
        pages_iter = rest_client.paginate(
            "/posts_offset_limit",
            paginator=OffsetPaginator(offset=0, limit=5, total_path="total_records"),
        )

        pages = list(pages_iter)

        assert_pagination(pages)

    def test_stop_after_empty_page(self):
        paginator = OffsetPaginator(
            offset=0,
            limit=50,
            maximum_offset=100,
            total_path=None,
            stop_after_empty_page=True,
        )
        response = Mock(Response, json=lambda: {"items": []})
        no_data_found: List[Any] = []
        paginator.update_state(response, no_data_found)  # Page 1
        assert paginator.has_next_page is False

    def test_guarantee_termination(self):
        OffsetPaginator(
            limit=10,
            total_path=None,
        )

        OffsetPaginator(
            limit=10,
            total_path=None,
            maximum_offset=1,
            stop_after_empty_page=False,
        )

        with pytest.raises(ValueError) as e:
            OffsetPaginator(
                limit=10,
                total_path=None,
                stop_after_empty_page=False,
            )
        assert e.match(
            "`total_path` or `maximum_offset` or `stop_after_empty_page` must be provided"
        )

        with pytest.raises(ValueError) as e:
            OffsetPaginator(
                limit=10,
                total_path=None,
                stop_after_empty_page=False,
                maximum_offset=None,
            )
        assert e.match(
            "`total_path` or `maximum_offset` or `stop_after_empty_page` must be provided"
        )


@pytest.mark.usefixtures("mock_api_server")
class TestPageNumberPaginator:
    def test_update_state(self):
        paginator = PageNumberPaginator(base_page=1, page=1, total_path="total_pages")
        response = Mock(Response, json=lambda: {"total_pages": 3})
        paginator.update_state(response, data=NON_EMPTY_PAGE)
        assert paginator.current_value == 2
        assert paginator.has_next_page is True

        paginator.update_state(response, data=NON_EMPTY_PAGE)
        assert paginator.current_value == 3
        assert paginator.has_next_page is True

        # Test for reaching the end
        paginator.update_state(response, data=NON_EMPTY_PAGE)
        assert paginator.has_next_page is False

    def test_init_request(self):
        paginator = PageNumberPaginator(base_page=1, total_path=None)
        request = Mock(Request)
        request.params = {}
        response = Mock(Response, json=lambda: "OK")

        assert paginator.current_value == 1
        assert paginator.has_next_page is True
        paginator.init_request(request)

        paginator.update_state(response, data=NON_EMPTY_PAGE)
        paginator.update_request(request)

        assert paginator.current_value == 2
        assert paginator.has_next_page is True
        assert request.params["page"] == 2

        paginator.update_state(response, data=None)
        paginator.update_request(request)

        assert paginator.current_value == 2
        assert paginator.has_next_page is False

        paginator.init_request(request)
        assert paginator.current_value == 1
        assert paginator.has_next_page is True

    def test_update_state_with_string_total_pages(self):
        paginator = PageNumberPaginator(base_page=1, page=1)
        response = Mock(Response, json=lambda: {"total": "3"})
        paginator.update_state(response, data=NON_EMPTY_PAGE)
        assert paginator.current_value == 2
        assert paginator.has_next_page is True

    def test_update_state_with_invalid_total_pages(self):
        paginator = PageNumberPaginator(base_page=1, page=1)
        response = Mock(Response, json=lambda: {"total_pages": "invalid"})
        with pytest.raises(ValueError):
            paginator.update_state(response, data=NON_EMPTY_PAGE)

    def test_update_state_without_total_pages(self):
        paginator = PageNumberPaginator(base_page=1, page=1)
        response = Mock(Response, json=lambda: {})
        with pytest.raises(ValueError):
            paginator.update_state(response, data=NON_EMPTY_PAGE)

    def test_update_request(self):
        paginator = PageNumberPaginator(base_page=1, page=1, page_param="page")
        request = Mock(Request)
        response = Mock(Response, json=lambda: {"total": 3})
        paginator.update_state(response, data=NON_EMPTY_PAGE)
        request.params = {}
        paginator.update_request(request)
        assert request.params["page"] == 2
        paginator.update_state(response, data=NON_EMPTY_PAGE)
        paginator.update_request(request)
        assert request.params["page"] == 3

    def test_maximum_page(self):
        paginator = PageNumberPaginator(base_page=1, page=1, maximum_page=3, total_path=None)
        response = Mock(Response, json=lambda: {"items": []})
        paginator.update_state(response, data=NON_EMPTY_PAGE)  # Page 1
        assert paginator.current_value == 2
        assert paginator.has_next_page is True

        paginator.update_state(response, data=NON_EMPTY_PAGE)  # Page 2
        assert paginator.current_value == 3
        assert paginator.has_next_page is False

    def test_stop_after_empty_page(self):
        paginator = PageNumberPaginator(
            base_page=1,
            page=1,
            maximum_page=5,
            stop_after_empty_page=True,
            total_path=None,
        )
        response = Mock(Response, json=lambda: {"items": []})
        no_data_found: List[Any] = []
        assert paginator.has_next_page is True
        paginator.update_state(response, no_data_found)
        assert paginator.current_value == 1
        assert paginator.has_next_page is False

    def test_client_pagination_one_based(self, rest_client):
        pages_iter = rest_client.paginate(
            "/posts",
            paginator=PageNumberPaginator(base_page=1, page=1, total_path="total_pages"),
        )

        pages = list(pages_iter)

        assert_pagination(pages)

    def test_client_pagination_one_based_default_page(self, rest_client):
        pages_iter = rest_client.paginate(
            "/posts",
            paginator=PageNumberPaginator(base_page=1, total_path="total_pages"),
        )

        pages = list(pages_iter)

        assert_pagination(pages)

    def test_client_pagination_zero_based(self, rest_client):
        pages_iter = rest_client.paginate(
            "/posts_zero_based",
            paginator=PageNumberPaginator(base_page=0, page=0, total_path="total_pages"),
        )

        pages = list(pages_iter)

        assert_pagination(pages)

    def test_guarantee_termination(self):
        PageNumberPaginator(
            total_path=None,
        )

        PageNumberPaginator(
            total_path=None,
            maximum_page=1,
            stop_after_empty_page=False,
        )

        with pytest.raises(ValueError) as e:
            PageNumberPaginator(
                total_path=None,
                stop_after_empty_page=False,
            )
        assert e.match("`total_path` or `maximum_page` or `stop_after_empty_page` must be provided")

        with pytest.raises(ValueError) as e:
            PageNumberPaginator(
                total_path=None,
                stop_after_empty_page=False,
                maximum_page=None,
            )
        assert e.match("`total_path` or `maximum_page` or `stop_after_empty_page` must be provided")


@pytest.mark.usefixtures("mock_api_server")
class TestJSONResponseCursorPaginator:
    def test_update_state(self):
        paginator = JSONResponseCursorPaginator(cursor_path="next_cursor")
        response = Mock(Response, json=lambda: {"next_cursor": "cursor-2", "results": []})
        paginator.update_state(response)
        assert paginator._next_reference == "cursor-2"
        assert paginator.has_next_page is True

    def test_update_request(self):
        paginator = JSONResponseCursorPaginator(cursor_path="next_cursor")
        paginator._next_reference = "cursor-2"
        request = Request(method="GET", url="http://example.com/api/resource")
        paginator.update_request(request)
        assert request.params["cursor"] == "cursor-2"

    def test_client_pagination(self, rest_client):
        pages_iter = rest_client.paginate(
            "/posts_cursor",
            paginator=JSONResponseCursorPaginator(cursor_path="next_cursor"),
        )

        pages = list(pages_iter)

        assert_pagination(pages)
