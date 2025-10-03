from typing import Any, List
from unittest.mock import Mock

import pytest

from requests.models import Response, Request
from requests import Session

from dlt.sources.helpers.rest_client.paginators import (
    SinglePagePaginator,
    OffsetPaginator,
    PageNumberPaginator,
    RangePaginator,
    HeaderLinkPaginator,
    JSONLinkPaginator,
    JSONResponseCursorPaginator,
    HeaderCursorPaginator,
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
            # Test escaping special characters in JSONPath
            {
                "next_url_path": "['@odata.nextLink']",
                "response_json": {
                    "@odata.nextLink": "http://example.com/odata-nextlink",
                    "value": [],
                },
                "expected": {
                    "next_reference": "http://example.com/odata-nextlink",
                    "has_next_page": True,
                },
            },
            # Test escaping special characters in JSONPath: alternate quotes
            {
                "next_url_path": '["@odata.nextLink"]',
                "response_json": {
                    "@odata.nextLink": "http://example.com/odata-nextlink",
                    "value": [],
                },
                "expected": {
                    "next_reference": "http://example.com/odata-nextlink",
                    "has_next_page": True,
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


class TestRangePaginator:
    def test_init(self) -> None:
        # param_name provided, param_body_path not provided
        RangePaginator(
            param_name="page",
            initial_value=0,
            value_step=1,
            total_path="total",
        )

        # param_name not provided, param_body_path provided
        RangePaginator(
            param_name=None,
            param_body_path="body.path",
            initial_value=0,
            value_step=1,
            total_path="total",
        )

        # both param_name and param_body_path provided
        with pytest.raises(
            ValueError, match="Either 'param_name' or 'param_body_path' must be provided, not both"
        ):
            RangePaginator(
                param_name="page",
                param_body_path="body.path",
                initial_value=0,
                value_step=1,
                total_path="total",
            )

        # both param_name and param_body_path are None or empty strings
        for param_name, param_body_path in [
            (None, None),
            ("", None),
            (None, ""),
            ("", ""),
        ]:
            with pytest.raises(
                ValueError,
                match="Either 'param_name' or 'param_body_path' must be provided, not both",
            ):
                RangePaginator(
                    param_name=param_name,
                    param_body_path=param_body_path,
                    initial_value=0,
                    value_step=1,
                    total_path="total",
                )

        with pytest.raises(
            ValueError, match="Either 'param_name' or 'param_body_path' must be provided, not both"
        ):
            RangePaginator(
                param_name=None,
                param_body_path=None,
                initial_value=0,
                value_step=1,
                total_path="total",
            )

    def test_range_paginator_update(self) -> None:
        request = Mock(Request)
        request.json = {}
        with pytest.raises(ValueError, match="param_name.*must not be empty"):
            RangePaginator._update_request_with_param_name(request, "", 0)

        with pytest.raises(ValueError, match="body_path.*must not be empty"):
            RangePaginator._update_request_with_body_path(request, "", 0)


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

    def test_page_param_initialization(self):
        # Test that offset_param defaults to 'offset' when offset_param and offset_body_path are None
        paginator = OffsetPaginator(limit=100)
        assert paginator.param_name == "offset"
        assert paginator.param_body_path is None

        # Test that page_param is set to the provided value
        paginator = OffsetPaginator(limit=100, offset_param="offset_param")
        assert paginator.param_name == "offset_param"
        assert paginator.param_body_path is None

        # Test that page_body_path is set to the provided value
        paginator = OffsetPaginator(limit=100, offset_body_path="offset_body_path")
        assert paginator.param_name is None
        assert paginator.param_body_path == "offset_body_path"

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

    def test_update_state_with_has_more(self):
        paginator = OffsetPaginator(0, 10, total_path=None, has_more_path="has_more")
        response = Mock(Response, json=lambda: {"has_more": False})
        paginator.update_state(response, data=NON_EMPTY_PAGE)
        assert paginator.has_next_page is False

    def test_update_state_with_string_has_more(self):
        paginator = OffsetPaginator(0, 10, total_path=None, has_more_path="has_more")
        response = Mock(Response, json=lambda: {"has_more": "true"})
        paginator.update_state(response, data=NON_EMPTY_PAGE)
        assert paginator.has_next_page is True

    def test_update_state_with_invalid_has_more(self):
        paginator = OffsetPaginator(0, 10, total_path=None, has_more_path="has_more")
        response = Mock(Response, json=lambda: {"has_more": "invalid"})
        with pytest.raises(ValueError):
            paginator.update_state(response, data=NON_EMPTY_PAGE)

    def test_update_state_without_has_more(self):
        paginator = OffsetPaginator(0, 10, total_path=None, has_more_path="has_more")
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

    def test_both_offset_param_and_offset_body_path_raises_error(self):
        # Test that an error is raised when both offset_param and offset_body_path are provided
        with pytest.raises(ValueError) as excinfo:
            OffsetPaginator(limit=100, offset_param="cursor", offset_body_path="page")
        assert "Either 'offset_param' or 'offset_body_path' must be provided, not both" in str(
            excinfo.value
        )

    def test_update_request_json(self):
        paginator = OffsetPaginator(limit=100, offset_body_path="offset")
        paginator.current_value = 2
        request = Request(method="POST", url="http://example.com/api/resource")
        paginator.update_request(request)
        assert request.json["offset"] == 2

    def test_update_request_json_nested(self):
        paginator = OffsetPaginator(limit=100, offset_body_path="pagination.offset")
        paginator.current_value = 2
        request = Request(method="POST", url="http://example.com/api/resource")
        paginator.update_request(request)
        assert request.json["pagination"]["offset"] == 2

    def test_update_request_json_with_existing_json(self):
        paginator = OffsetPaginator(limit=100, offset_body_path="offset")
        paginator.current_value = 2
        request = Request(
            method="POST", url="http://example.com/api/resource", json={"existing": "data"}
        )
        paginator.update_request(request)
        assert request.json["existing"] == "data"
        assert request.json["offset"] == 2

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

    def test_client_pagination_with_json(self, rest_client):
        pages_iter = rest_client.paginate(
            "/posts_offset_limit_via_json_body",
            method="POST",
            json={"limit": 5, "offset": 0, "ids_greater_than": -1},
            paginator=OffsetPaginator(
                offset=0,
                limit=5,
                offset_body_path="offset",
                limit_body_path="limit",
                total_path="total_records",
            ),
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

        OffsetPaginator(
            limit=10,
            total_path=None,
            has_more_path="has_more",
            stop_after_empty_page=False,
        )

        with pytest.raises(ValueError) as e:
            OffsetPaginator(
                limit=10,
                total_path=None,
                stop_after_empty_page=False,
            )
        assert e.match(
            "`total_path`, `maximum_offset`, `has_more_path`, or `stop_after_empty_page`"
            " must be provided"
        )

        with pytest.raises(ValueError) as e:
            OffsetPaginator(
                limit=10,
                total_path=None,
                stop_after_empty_page=False,
                maximum_offset=None,
            )
        assert e.match(
            "`total_path`, `maximum_offset`, `has_more_path`, or `stop_after_empty_page`"
            " must be provided"
        )

        with pytest.raises(ValueError) as e:
            OffsetPaginator(
                limit=10,
                total_path=None,
                stop_after_empty_page=False,
                maximum_offset=None,
                has_more_path=None,
            )
        assert e.match(
            "`total_path`, `maximum_offset`, `has_more_path`, or `stop_after_empty_page`"
            " must be provided"
        )

    @pytest.mark.parametrize(
        "offset_param,offset_body_path",
        [
            (None, None),
            ("", None),
            (None, ""),
            ("", ""),
        ],
    )
    def test_empty_offset_params(self, offset_param, offset_body_path) -> None:
        if offset_param is None and offset_body_path is None:
            # Fallback to default 'offset' param
            paginator = OffsetPaginator(
                limit=100, offset_param=offset_param, offset_body_path=offset_body_path
            )
            assert paginator.param_name == "offset"
            assert paginator.param_body_path is None
        else:
            with pytest.raises(
                ValueError, match="Either 'offset_param' or 'offset_body_path' must be provided.*"
            ):
                OffsetPaginator(
                    limit=100,
                    offset_param=offset_param,
                    offset_body_path=offset_body_path,
                )

    @pytest.mark.parametrize(
        "limit_param,limit_body_path",
        [
            (None, None),
            ("", None),
            (None, ""),
            ("", ""),
        ],
    )
    def test_empty_limit_params(self, limit_param, limit_body_path) -> None:
        if limit_param is None and limit_body_path is None:
            # Fallback to default 'limit' param
            paginator = OffsetPaginator(
                limit=100, limit_param=limit_param, limit_body_path=limit_body_path
            )
            assert paginator.limit_param == "limit"
            assert paginator.limit_body_path is None
        else:
            with pytest.raises(
                ValueError, match="Either 'limit_param' or 'limit_body_path' must be provided.*"
            ):
                OffsetPaginator(
                    limit=100,
                    limit_param=limit_param,
                    limit_body_path=limit_body_path,
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

    def test_page_param_initialization(self):
        # Test that page_param defaults to 'page' when page_param and page_body_path are None
        paginator = PageNumberPaginator()
        assert paginator.param_name == "page"
        assert paginator.param_body_path is None

        # Test that page_param is set to the provided value
        paginator = PageNumberPaginator(page_param="page_param")
        assert paginator.param_name == "page_param"
        assert paginator.param_body_path is None

        # Test that page_body_path is set to the provided value
        paginator = PageNumberPaginator(page_body_path="page_body_path")
        assert paginator.param_name is None
        assert paginator.param_body_path == "page_body_path"

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

    def test_both_page_param_and_page_body_path_raises_error(self):
        # Test that an error is raised when both page_param and page_body_path are provided
        with pytest.raises(ValueError) as excinfo:
            PageNumberPaginator(page_param="cursor", page_body_path="page")
        assert "Either 'page_param' or 'page_body_path' must be provided, not both" in str(
            excinfo.value
        )

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

    def test_update_state_with_has_more(self):
        paginator = PageNumberPaginator(page=1, total_path=None, has_more_path="has_more")
        response = Mock(Response, json=lambda: {"has_more": False})
        paginator.update_state(response, data=NON_EMPTY_PAGE)
        assert paginator.has_next_page is False

    def test_update_state_with_string_has_more(self):
        paginator = PageNumberPaginator(page=1, total_path=None, has_more_path="has_more")
        response = Mock(Response, json=lambda: {"has_more": "true"})
        paginator.update_state(response, data=NON_EMPTY_PAGE)
        assert paginator.has_next_page is True

    def test_update_state_with_invalid_has_more(self):
        paginator = PageNumberPaginator(page=1, total_path=None, has_more_path="has_more")
        response = Mock(Response, json=lambda: {"has_more": "invalid"})
        with pytest.raises(ValueError):
            paginator.update_state(response, data=NON_EMPTY_PAGE)

    def test_update_state_without_has_more(self):
        paginator = PageNumberPaginator(page=1, total_path=None, has_more_path="has_more")
        response = Mock(Response, json=lambda: {})
        with pytest.raises(ValueError):
            paginator.update_state(response, data=NON_EMPTY_PAGE)

    def test_update_request_param(self):
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

    def test_update_request_json(self):
        paginator = PageNumberPaginator(base_page=1, page=1, page_body_path="page")
        paginator.current_value = 2
        request = Request(method="POST", url="http://example.com/api/resource")
        paginator.update_request(request)
        assert request.json["page"] == 2

    def test_update_request_json_nested(self):
        paginator = PageNumberPaginator(base_page=1, page=1, page_body_path="page.next")
        paginator.current_value = 2
        request = Request(method="POST", url="http://example.com/api/resource")
        paginator.update_request(request)
        assert request.json["page"]["next"] == 2

    def test_update_request_json_with_existing_json(self):
        paginator = PageNumberPaginator(base_page=1, page=1, page_body_path="page")
        paginator.current_value = 2
        request = Request(
            method="POST", url="http://example.com/api/resource", json={"existing": "data"}
        )
        paginator.update_request(request)
        assert request.json["existing"] == "data"
        assert request.json["page"] == 2

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

    def test_client_pagination_with_json(self, rest_client):
        pages_iter = rest_client.paginate(
            "/posts/search",
            method="POST",
            json={"page_size": 5, "page": 1, "ids_greater_than": -1},
            paginator=PageNumberPaginator(
                base_page=1, page_body_path="page", total_path="total_pages"
            ),
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
        assert e.match(
            "`total_path`, `maximum_page`, `has_more_path`, or `stop_after_empty_page`"
            " must be provided"
        )

        with pytest.raises(ValueError) as e:
            PageNumberPaginator(
                total_path=None,
                stop_after_empty_page=False,
                maximum_page=None,
            )
        assert e.match(
            "`total_path`, `maximum_page`, `has_more_path`, or `stop_after_empty_page`"
            " must be provided"
        )

        with pytest.raises(ValueError) as e:
            PageNumberPaginator(
                total_path=None,
                stop_after_empty_page=False,
                maximum_page=None,
                has_more_path=None,
            )
        assert e.match(
            "`total_path`, `maximum_page`, `has_more_path`, or `stop_after_empty_page`"
            " must be provided"
        )

    @pytest.mark.parametrize(
        "page_param,page_body_path",
        [
            (None, None),
            ("", None),
            (None, ""),
            ("", ""),
        ],
    )
    def test_empty_params(self, page_param, page_body_path) -> None:
        if page_param is None and page_body_path is None:
            # Fallback to default 'page' param
            paginator = PageNumberPaginator(page_param=page_param, page_body_path=page_body_path)
            assert paginator.param_name == "page"
            assert paginator.param_body_path is None
        else:
            with pytest.raises(
                ValueError, match="Either 'page_param' or 'page_body_path' must be provided.*"
            ):
                PageNumberPaginator(
                    page_param=page_param,
                    page_body_path=page_body_path,
                )


@pytest.mark.usefixtures("mock_api_server")
class TestJSONResponseCursorPaginator:
    def test_update_state(self):
        paginator = JSONResponseCursorPaginator(cursor_path="next_cursor")
        response = Mock(Response, json=lambda: {"next_cursor": "cursor-2", "results": []})
        paginator.update_state(response)
        assert paginator._next_reference == "cursor-2"
        assert paginator.has_next_page is True

    def test_cursor_param_initialization(self):
        # Test that cursor_param defaults to 'cursor' when both cursor_param and cursor_body_path are None
        paginator = JSONResponseCursorPaginator()
        assert paginator.cursor_param == "cursor"
        assert paginator.cursor_body_path is None

        # Test that cursor_param is set to the provided value
        paginator = JSONResponseCursorPaginator(cursor_param="cursor_param")
        assert paginator.cursor_param == "cursor_param"
        assert paginator.cursor_body_path is None

        # Test that cursor_body_path is set to the provided value
        paginator = JSONResponseCursorPaginator(cursor_body_path="cursor_body_path")
        assert paginator.cursor_param is None
        assert paginator.cursor_body_path == "cursor_body_path"

    def test_both_cursor_param_and_cursor_body_path_raises_error(self):
        # Test that an error is raised when both cursor_param and cursor_body_path are provided
        with pytest.raises(ValueError) as excinfo:
            JSONResponseCursorPaginator(
                cursor_path="next_cursor", cursor_param="cursor", cursor_body_path="page"
            )
        assert "Either 'cursor_param' or 'cursor_body_path' must be provided, not both" in str(
            excinfo.value
        )

    def test_update_state_when_cursor_path_is_empty_string(self):
        paginator = JSONResponseCursorPaginator(cursor_path="next_cursor")
        response = Mock(Response, json=lambda: {"next_cursor": "", "results": []})
        paginator.update_state(response)
        assert paginator.has_next_page is False

    def test_update_request_param(self):
        paginator = JSONResponseCursorPaginator(cursor_path="next_cursor")
        paginator._next_reference = "cursor-2"
        request = Request(method="GET", url="http://example.com/api/resource")
        paginator.update_request(request)
        assert request.params["cursor"] == "cursor-2"

    def test_update_request_json(self):
        paginator = JSONResponseCursorPaginator(
            cursor_path="next_cursor", cursor_body_path="cursor"
        )
        paginator._next_reference = "cursor-2"
        request = Request(method="POST", url="http://example.com/api/resource")
        paginator.update_request(request)
        assert request.json["cursor"] == "cursor-2"

    def test_update_request_json_nested(self):
        paginator = JSONResponseCursorPaginator(
            cursor_path="next_cursor", cursor_body_path="cursor.next"
        )
        paginator._next_reference = "cursor-2"
        request = Request(method="POST", url="http://example.com/api/resource")
        paginator.update_request(request)
        assert request.json["cursor"]["next"] == "cursor-2"

    def test_update_request_json_with_existing_json(self):
        paginator = JSONResponseCursorPaginator(
            cursor_path="next_cursor", cursor_body_path="cursor"
        )
        paginator._next_reference = "cursor-2"
        request = Request(
            method="POST", url="http://example.com/api/resource", json={"existing": "data"}
        )
        paginator.update_request(request)
        assert request.json["existing"] == "data"
        assert request.json["cursor"] == "cursor-2"

    def test_client_pagination_with_param(self, rest_client):
        pages_iter = rest_client.paginate(
            "/posts_cursor",
            paginator=JSONResponseCursorPaginator(cursor_path="next_cursor"),
        )

        pages = list(pages_iter)

        assert_pagination(pages)

    def test_client_pagination_with_json(self, rest_client):
        pages_iter = rest_client.paginate(
            "/posts/search",
            method="POST",
            json={"ids_greater_than": -1, "page_size": 5, "page_count": 5},
            paginator=JSONResponseCursorPaginator(cursor_path="next_page", cursor_body_path="page"),
        )

        pages = list(pages_iter)

        assert_pagination(pages)


@pytest.mark.usefixtures("mock_api_server")
class TestHeaderCursorPaginator:
    def test_update_state(self):
        paginator = HeaderCursorPaginator(cursor_key="next_cursor")
        response = Mock(Response)
        response.headers = {"next_cursor": "cursor-2"}
        paginator.update_state(response)
        assert paginator._next_reference == "cursor-2"
        assert paginator.has_next_page is True

    def test_update_state_when_cursor_path_is_empty_string(self):
        paginator = HeaderCursorPaginator(cursor_key="next_cursor")
        response = Mock(Response)
        response.headers = {}
        paginator.update_state(response)
        assert paginator.has_next_page is False

    def test_update_request(self):
        paginator = HeaderCursorPaginator(cursor_key="next_cursor", cursor_param="cursor")
        response = Mock(Response)
        response.headers = {"next_cursor": "cursor-2"}
        paginator.update_state(response)
        request = Request(method="GET", url="http://example.com/api/resource")
        paginator.update_request(request)
        assert request.params["cursor"] == "cursor-2"

    def test_client_pagination(self, rest_client):
        pages_iter = rest_client.paginate(
            "/posts_header_cursor",
            paginator=HeaderCursorPaginator(cursor_key="cursor", cursor_param="page"),
        )

        pages = list(pages_iter)

        assert_pagination(pages)
