import os
from typing import Any, cast

import pytest
from requests import PreparedRequest, Request, Response
from requests.auth import AuthBase
from requests.exceptions import HTTPError

from dlt.common import logger
from dlt.common.typing import TSecretStrValue
from dlt.sources.helpers.requests import Client
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import (
    APIKeyAuth,
    AuthConfigBase,
    BearerTokenAuth,
    HttpBasicAuth,
    OAuth2ClientCredentialsFlow,
    OAuthJWTAuth,
)
from dlt.sources.helpers.rest_client.client import Hooks
from dlt.sources.helpers.rest_client.exceptions import IgnoreResponseException
from dlt.sources.helpers.rest_client.paginators import JSONResponsePaginator

from .conftest import assert_pagination


def load_private_key(name="private_key.pem"):
    key_path = os.path.join(os.path.dirname(__file__), name)
    with open(key_path, "r", encoding="utf-8") as key_file:
        return key_file.read()


TEST_PRIVATE_KEY = load_private_key()


def build_rest_client(auth=None) -> RESTClient:
    return RESTClient(
        base_url="https://api.example.com",
        headers={"Accept": "application/json"},
        session=Client().session,
        auth=auth,
    )


@pytest.fixture
def rest_client() -> RESTClient:
    return build_rest_client()


@pytest.fixture
def rest_client_oauth() -> RESTClient:
    auth = OAuth2ClientCredentialsExample(
        access_token_request_data={
            "grant_type": "client_credentials",
        },
        client_id=cast(TSecretStrValue, "test-client-id"),
        client_secret=cast(TSecretStrValue, "test-client-secret"),
    )
    return build_rest_client(auth=auth)


class OAuth2ClientCredentialsExample(OAuth2ClientCredentialsFlow):
    def build_access_token_request(self):
        return {
            "url": "https://api.example.com/oauth/token",
            "headers": {
                "Content-Type": "application/x-www-form-urlencoded",
            },
            "data": {
                **self.access_token_request_data,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
        }


@pytest.fixture
def rest_client_immediate_oauth_expiry(auth=None) -> RESTClient:
    class OAuth2ClientCredentialsExpiringNow(OAuth2ClientCredentialsFlow):
        def build_access_token_request(self):
            return {
                "url": "https://api.example.com/oauth/token-expires-now",
                "headers": {
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                "data": {
                    **self.access_token_request_data,
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
            }

    auth = OAuth2ClientCredentialsExpiringNow(
        access_token_request_data={
            "grant_type": "client_credentials",
        },
        client_id=cast(TSecretStrValue, "test-client-id"),
        client_secret=cast(TSecretStrValue, "test-client-secret"),
    )

    return build_rest_client(auth=auth)


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
        ):
            # response that produced data
            assert isinstance(page.response, Response)
            # updated request
            assert isinstance(page.request, Request)
            # make request url should be same as next link in paginator
            if page.paginator.has_next_page:
                paginator = cast(JSONResponsePaginator, page.paginator)
                assert paginator._next_reference == page.request.url

    def test_default_paginator(self, rest_client: RESTClient):
        pages_iter = rest_client.paginate("/posts")

        pages = list(pages_iter)

        assert_pagination(pages)

    def test_excplicit_paginator(self, rest_client: RESTClient):
        pages_iter = rest_client.paginate(
            "/posts", paginator=JSONResponsePaginator(next_url_path="next_page")
        )
        pages = list(pages_iter)

        assert_pagination(pages)

    def test_excplicit_paginator_relative_next_url(self, rest_client: RESTClient):
        pages_iter = rest_client.paginate(
            "/posts_relative_next_url",
            paginator=JSONResponsePaginator(next_url_path="next_page"),
        )
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
            auth=APIKeyAuth(name="x-api-key", api_key=cast(TSecretStrValue, "test-api-key")),
        )
        assert response.status_code == 200
        assert response.json()["data"][0] == {"id": 0, "title": "Post 0"}

    def test_oauth2_client_credentials_flow_auth_success(self, rest_client_oauth: RESTClient):
        response = rest_client_oauth.get("/protected/posts/bearer-token")

        assert response.status_code == 200
        assert "test-token" in response.request.headers["Authorization"]

        pages_iter = rest_client_oauth.paginate("/protected/posts/bearer-token")

        assert_pagination(list(pages_iter))

    def test_oauth2_client_credentials_flow_wrong_client_id(self, rest_client: RESTClient):
        auth = OAuth2ClientCredentialsExample(
            access_token_request_data={"grant_type": "client_credentials"},
            client_id=cast(TSecretStrValue, "invalid-client-id"),
            client_secret=cast(TSecretStrValue, "test-client-secret"),
        )

        with pytest.raises(HTTPError) as e:
            rest_client.get("/protected/posts/bearer-token", auth=auth)
        assert e.type == HTTPError
        assert e.match("401 Client Error")

    def test_oauth2_client_credentials_flow_wrong_client_secret(self, rest_client: RESTClient):
        auth = OAuth2ClientCredentialsExample(
            access_token_request_data={"grant_type": "client_credentials"},
            client_id=cast(TSecretStrValue, "test-client-id"),
            client_secret=cast(TSecretStrValue, "invalid-client-secret"),
        )

        with pytest.raises(HTTPError) as e:
            rest_client.get(
                "/protected/posts/bearer-token",
                auth=auth,
            )
        assert e.type == HTTPError
        assert e.match("401 Client Error")

    def test_oauth_token_expired_refresh(self, rest_client_immediate_oauth_expiry: RESTClient):
        rest_client = rest_client_immediate_oauth_expiry
        assert rest_client.auth.access_token is None
        response = rest_client.get("/protected/posts/bearer-token")
        assert response.status_code == 200
        assert rest_client.auth.access_token is not None
        expiry_0 = rest_client.auth.token_expiry
        rest_client.auth.token_expiry = rest_client.auth.token_expiry.subtract(seconds=1)
        expiry_1 = rest_client.auth.token_expiry
        assert expiry_0 > expiry_1
        assert rest_client.auth.is_token_expired()

        response = rest_client.get("/protected/posts/bearer-token")
        assert response.status_code == 200
        expiry_2 = rest_client.auth.token_expiry
        assert expiry_2 > expiry_1
        assert response.json()["data"][0] == {"id": 0, "title": "Post 0"}

    def test_oauth_jwt_auth_success(self, rest_client: RESTClient):
        auth = OAuthJWTAuth(
            client_id="test-client-id",
            private_key=TEST_PRIVATE_KEY,
            auth_endpoint="https://api.example.com/oauth/token",
            scopes=["read", "write"],
            headers={"Content-Type": "application/json"},
            session=Client().session,
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

    def test_custom_session_client(self, mocker):
        mocked_warning = mocker.patch.object(logger, "warning")
        RESTClient(
            base_url="https://api.example.com",
            headers={"Accept": "application/json"},
            session=Client(raise_for_status=True).session,
        )
        assert (
            mocked_warning.call_args[0][0]
            == "The session provided has raise_for_status enabled. This may cause unexpected"
            " behavior."
        )

    def test_custom_auth_success(self, rest_client: RESTClient):
        class CustomAuthConfigBase(AuthConfigBase):
            def __init__(self, token: str):
                self.token = token

            def __call__(self, request: PreparedRequest) -> PreparedRequest:
                request.headers["Authorization"] = f"Bearer {self.token}"
                return request

        class CustomAuthAuthBase(AuthBase):
            def __init__(self, token: str):
                self.token = token

            def __call__(self, request: PreparedRequest) -> PreparedRequest:
                request.headers["Authorization"] = f"Bearer {self.token}"
                return request

        auth_list = [
            CustomAuthConfigBase("test-token"),
            CustomAuthAuthBase("test-token"),
        ]

        for auth in auth_list:
            response = rest_client.get(
                "/protected/posts/bearer-token",
                auth=auth,
            )

            assert response.status_code == 200
            assert response.json()["data"][0] == {"id": 0, "title": "Post 0"}

            pages_iter = rest_client.paginate(
                "/protected/posts/bearer-token",
                auth=auth,
            )

            pages_list = list(pages_iter)
            assert_pagination(pages_list)

            assert pages_list[0].response.request.headers["Authorization"] == "Bearer test-token"

    def test_send_request_allows_ca_bundle(self, mocker, rest_client):
        mocker.patch.dict(os.environ, {"REQUESTS_CA_BUNDLE": "/path/to/some/ca-bundle"})

        _send = rest_client.session.send

        def _fake_send(*args, **kwargs):
            assert kwargs["verify"] == "/path/to/some/ca-bundle"
            return _send(*args, **kwargs)

        rest_client.session.send = _fake_send

        result = rest_client.get("/posts/1")
        assert result.status_code == 200
