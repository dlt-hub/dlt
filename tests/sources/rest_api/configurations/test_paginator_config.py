from typing import get_args

import pytest

import dlt
import dlt.common
import dlt.common.exceptions
import dlt.extract
from dlt.common.jsonpath import compile_path
from dlt.sources.helpers.rest_client.paginators import (
    HeaderLinkPaginator,
    JSONResponseCursorPaginator,
    JSONResponsePaginator,
    OffsetPaginator,
    PageNumberPaginator,
)
from dlt.sources.rest_api import (
    rest_api_source,
)
from dlt.sources.rest_api.config_setup import (
    PAGINATOR_MAP,
    create_paginator,
)
from dlt.sources.rest_api.typing import (
    PaginatorConfig,
    PaginatorType,
    RESTAPIConfig,
)

try:
    from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator
except ImportError:
    from dlt.sources.helpers.rest_client.paginators import (
        JSONResponsePaginator as JSONLinkPaginator,
    )


from .source_configs import (
    PAGINATOR_TYPE_CONFIGS,
)


@pytest.mark.parametrize("paginator_type", get_args(PaginatorType))
def test_paginator_shorthands(paginator_type: PaginatorConfig) -> None:
    try:
        create_paginator(paginator_type)
    except ValueError as v_ex:
        # offset paginator cannot be instantiated
        assert paginator_type == "offset"
        assert "offset" in str(v_ex)


@pytest.mark.parametrize("paginator_type_config", PAGINATOR_TYPE_CONFIGS)
def test_paginator_type_configs(paginator_type_config: PaginatorConfig) -> None:
    paginator = create_paginator(paginator_type_config)
    if paginator_type_config["type"] == "auto":  # type: ignore[index]
        assert paginator is None
    else:
        # assert types and default params
        assert isinstance(paginator, PAGINATOR_MAP[paginator_type_config["type"]])  # type: ignore[index]
        # check if params are bound
        if isinstance(paginator, HeaderLinkPaginator):
            assert paginator.links_next_key == "next_page"
        if isinstance(paginator, PageNumberPaginator):
            assert paginator.current_value == 10
            assert paginator.base_index == 1
            assert paginator.param_name == "page"
            assert paginator.total_path == compile_path("response.pages")
            assert paginator.maximum_value is None
        if isinstance(paginator, OffsetPaginator):
            assert paginator.current_value == 0
            assert paginator.param_name == "offset"
            assert paginator.limit == 100
            assert paginator.limit_param == "limit"
            assert paginator.total_path == compile_path("total")
            assert paginator.maximum_value == 1000
        if isinstance(paginator, JSONLinkPaginator):
            assert paginator.next_url_path == compile_path("response.nex_page_link")
        if isinstance(paginator, JSONResponseCursorPaginator):
            assert paginator.cursor_path == compile_path("cursors.next")
            assert paginator.cursor_param == "cursor"


def test_paginator_instance_config() -> None:
    paginator = OffsetPaginator(limit=100)
    assert create_paginator(paginator) is paginator


def test_page_number_paginator_creation() -> None:
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.example.com",
            "paginator": {
                "type": "page_number",
                "page_param": "foobar",
                "total_path": "response.pages",
                "base_page": 1,
                "maximum_page": 5,
            },
        },
        "resources": ["posts"],
    }
    try:
        rest_api_source(config)
    except dlt.common.exceptions.DictValidationException:
        pytest.fail("DictValidationException was unexpectedly raised")


def test_allow_deprecated_json_response_paginator(mock_api_server) -> None:
    """
    Delete this test as soon as we stop supporting the deprecated key json_response
    for the JSONLinkPaginator
    """
    config: RESTAPIConfig = {
        "client": {"base_url": "https://api.example.com"},
        "resources": [
            {
                "name": "posts",
                "endpoint": {
                    "path": "posts",
                    "paginator": {
                        "type": "json_response",
                        "next_url_path": "links.next",
                    },
                },
            },
        ],
    }

    rest_api_source(config)


def test_allow_deprecated_json_response_paginator_2(mock_api_server) -> None:
    """
    Delete this test as soon as we stop supporting the deprecated key json_response
    for the JSONLinkPaginator
    """
    config: RESTAPIConfig = {
        "client": {"base_url": "https://api.example.com"},
        "resources": [
            {
                "name": "posts",
                "endpoint": {
                    "path": "posts",
                    "paginator": JSONResponsePaginator(next_url_path="links.next"),
                },
            },
        ],
    }

    rest_api_source(config)


def test_error_message_invalid_paginator() -> None:
    with pytest.raises(ValueError) as e:
        create_paginator("non_existing_method")  # type: ignore
    assert (
        str(e.value)
        == "Invalid paginator: non_existing_method. Available options: json_link, json_response,"
        " header_link, auto, single_page, cursor, offset, page_number."
    )
