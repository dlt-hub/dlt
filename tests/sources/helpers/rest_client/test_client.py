import os
import pytest
from typing import Any, cast
from dlt.common.typing import TSecretStrValue
from dlt.sources.helpers.requests import Response, Request
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.client import Hooks
from dlt.sources.helpers.rest_client.paginators import JSONResponsePaginator

from dlt.sources.helpers.rest_client.auth import AuthConfigBase
from dlt.sources.helpers.rest_client.auth import (
    BearerTokenAuth,
    APIKeyAuth,
    HttpBasicAuth,
    OAuthJWTAuth,
)
from dlt.sources.helpers.rest_client.exceptions import IgnoreResponseException

from .conftest import assert_pagination


def load_private_key(name="private_key.pem"):
    key_path = os.path.join(os.path.dirname(__file__), name)
    with open(key_path, "r", encoding="utf-8") as key_file:
        return key_file.read()


TEST_PRIVATE_KEY = load_private_key()


@pytest.fixture
def rest_client() -> RESTClient:
    return RESTClient(
        base_url="https://api.example.com",
        headers={"Accept": "application/json"},
    )


@pytest.mark.usefixtures("mock_api_server")
class TestRESTClient:
    def test_get_single_resource(self, rest_client):
        response = rest_client.get("/posts/1")
        assert response.status_code == 200
        assert response.json() == {"id": "1", "body": "Post body 1"}

    def test_pagination(self, rest_client: RESTClient):
        pages_iter = rest_client.paginate(
            "/posts",
            paginator=JSONResponsePaginator(next_url_path="next_page"),
        )

        pages = list(pages_iter)

        assert_pagination(pages)

    def test_page_context(self, rest_client: RESTClient) -> None:
        for page in rest_client.paginate(
            "/posts",
            paginator=JSONResponsePaginator(next_url_path="next_page"),
            auth=AuthConfigBase(),
        ):
            # response that produced data
            assert isinstance(page.response, Response)
            # updated request
            assert isinstance(page.request, Request)
            # make request url should be same as next link in paginator
            if page.paginator.has_next_page:
                assert page.paginator.next_reference == page.request.url

    def test_default_paginator(self, rest_client: RESTClient):
        pages_iter = rest_client.paginate("/posts")

        pages = list(pages_iter)

        assert_pagination(pages)

    def test_paginate_with_hooks(self, rest_client: RESTClient):
        def response_hook(response: Response, *args: Any, **kwargs: Any) -> None:
            if response.status_code == 404:
                raise IgnoreResponseException

        hooks: Hooks = {
            "response": response_hook,
        }

        pages_iter = rest_client.paginate(
            "/posts",
            paginator=JSONResponsePaginator(next_url_path="next_page"),
            hooks=hooks,
        )

        pages = list(pages_iter)

        assert_pagination(pages)

        pages_iter = rest_client.paginate(
            "/posts/1/some_details_404",
            paginator=JSONResponsePaginator(),
            hooks=hooks,
        )

        pages = list(pages_iter)
        assert pages == []

    def test_basic_auth_success(self, rest_client: RESTClient):
        response = rest_client.get(
            "/protected/posts/basic-auth",
            auth=HttpBasicAuth("user", cast(TSecretStrValue, "password")),
        )
        assert response.status_code == 200
        assert response.json()["data"][0] == {"id": 0, "title": "Post 0"}

        pages_iter = rest_client.paginate(
            "/protected/posts/basic-auth",
            auth=HttpBasicAuth("user", cast(TSecretStrValue, "password")),
        )

        pages = list(pages_iter)
        assert_pagination(pages)

    def test_bearer_token_auth_success(self, rest_client: RESTClient):
        response = rest_client.get(
            "/protected/posts/bearer-token",
            auth=BearerTokenAuth(cast(TSecretStrValue, "test-token")),
        )
        assert response.status_code == 200
        assert response.json()["data"][0] == {"id": 0, "title": "Post 0"}

        pages_iter = rest_client.paginate(
            "/protected/posts/bearer-token",
            auth=BearerTokenAuth(cast(TSecretStrValue, "test-token")),
        )

        pages = list(pages_iter)
        assert_pagination(pages)

    def test_api_key_auth_success(self, rest_client: RESTClient):
        response = rest_client.get(
            "/protected/posts/api-key",
            auth=APIKeyAuth(
                name="x-api-key", api_key=cast(TSecretStrValue, "test-api-key")
            ),
        )
        assert response.status_code == 200
        assert response.json()["data"][0] == {"id": 0, "title": "Post 0"}

    def test_oauth_jwt_auth_success(self, rest_client: RESTClient):
        auth = OAuthJWTAuth(
            client_id="test-client-id",
            private_key=TEST_PRIVATE_KEY,
            auth_endpoint="https://api.example.com/oauth/token",
            scopes=["read", "write"],
            headers={"Content-Type": "application/json"},
        )

        response = rest_client.get(
            "/protected/posts/bearer-token",
            auth=auth,
        )

        assert response.status_code == 200
        assert "test-token" in response.request.headers["Authorization"]

        pages_iter = rest_client.paginate(
            "/protected/posts/bearer-token",
            auth=auth,
        )

        assert_pagination(list(pages_iter))
