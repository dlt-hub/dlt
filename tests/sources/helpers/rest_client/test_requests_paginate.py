import pytest

from dlt.sources.helpers.rest_client import paginate
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator
from .conftest import assert_pagination


@pytest.mark.usefixtures("mock_api_server")
def test_requests_paginate():
    pages_iter = paginate(
        "https://api.example.com/posts",
        paginator=JSONLinkPaginator(next_url_path="next_page"),
    )

    pages = list(pages_iter)

    assert_pagination(pages)
