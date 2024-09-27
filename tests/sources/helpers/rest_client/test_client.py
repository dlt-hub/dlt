import os
from base64 import b64encode
from typing import Any, Dict, cast
from unittest.mock import patch, ANY

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
    OAuth2ClientCredentials,
    OAuthJWTAuth,
)
from dlt.sources.helpers.rest_client.client import Hooks
from dlt.sources.helpers.rest_client.exceptions import IgnoreResponseException
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator, BaseReferencePaginator

from .conftest import DEFAULT_PAGE_SIZE, DEFAULT_TOTAL_PAGES, assert_pagination


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
    auth = OAuth2ClientCredentials(
        access_token_url=cast(TSecretStrValue, "https://api.example.com/oauth/token"),
        client_id=cast(TSecretStrValue, "test-client-id"),
        client_secret=cast(TSecretStrValue, "test-client-secret"),
        session=Client().session,
    )
    return build_rest_client(auth=auth)


@pytest.fixture
def rest_client_immediate_oauth_expiry(auth=None) -> RESTClient:
    credentials_expiring_now = OAuth2ClientCredentials(
        access_token_url=cast(TSecretStrValue, "https://api.example.com/oauth/token-expires-now"),
        client_id=cast(TSecretStrValue, "test-client-id"),
        client_secret=cast(TSecretStrValue, "test-client-secret"),
        session=Client().session,
    )
    return build_rest_client(auth=credentials_expiring_now)


@pytest.mark.usefixtures("mock_api_server")
class TestRESTClient:
    def test_get_single_resource(self, rest_client):
        response = rest_client.get("/posts/1")
        assert response.status_code == 200
        assert response.json() == {"id": 1, "body": "Post body 1"}

    def test_pagination(self, rest_client: RESTClient):
        pages_iter = rest_client.paginate(
            "/posts",
            paginator=JSONLinkPaginator(next_url_path="next_page"),
        )

        pages = list(pages_iter)

        assert_pagination(pages)

    def test_page_context(self, rest_client: RESTClient) -> None:
        for page in rest_client.paginate(
            "/posts",
            paginator=JSONLinkPaginator(next_url_path="next_page"),
        ):
            # response that produced data
            assert isinstance(page.response, Response)
            # updated request
            assert isinstance(page.request, Request)
            # make request url should be same as next link in paginator
            if page.paginator.has_next_page:
                paginator = cast(JSONLinkPaginator, page.paginator)
                assert paginator._next_reference == page.request.url

    def test_default_paginator(self, rest_client: RESTClient):
        pages_iter = rest_client.paginate("/posts")

        pages = list(pages_iter)

        assert_pagination(pages)

    def test_excplicit_paginator(self, rest_client: RESTClient):
        pages_iter = rest_client.paginate(
            "/posts", paginator=JSONLinkPaginator(next_url_path="next_page")
        )
        pages = list(pages_iter)

        assert_pagination(pages)

    def test_excplicit_paginator_relative_next_url(self, rest_client: RESTClient):
        pages_iter = rest_client.paginate(
            "/posts_relative_next_url",
            paginator=JSONLinkPaginator(next_url_path="next_page"),
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
            paginator=JSONLinkPaginator(next_url_path="next_page"),
            hooks=hooks,
        )

        pages = list(pages_iter)

        assert_pagination(pages)

        pages_iter = rest_client.paginate(
            "/posts/1/some_details_404",
            paginator=JSONLinkPaginator(),
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
        auth = OAuth2ClientCredentials(
            access_token_url=cast(TSecretStrValue, "https://api.example.com/oauth/token"),
            client_id=cast(TSecretStrValue, "invalid-client-id"),
            client_secret=cast(TSecretStrValue, "test-client-secret"),
            session=Client().session,
        )

        with pytest.raises(HTTPError) as e:
            rest_client.get("/protected/posts/bearer-token", auth=auth)
        assert e.type == HTTPError
        assert e.match("401 Client Error")

    def test_oauth2_client_credentials_flow_wrong_client_secret(self, rest_client: RESTClient):
        auth = OAuth2ClientCredentials(
            access_token_url=cast(TSecretStrValue, "https://api.example.com/oauth/token"),
            client_id=cast(TSecretStrValue, "test-client-id"),
            client_secret=cast(TSecretStrValue, "invalid-client-secret"),
            session=Client().session,
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
        auth = cast(OAuth2ClientCredentials, rest_client.auth)

        with patch.object(auth, "obtain_token", wraps=auth.obtain_token) as mock_obtain_token:
            assert auth.access_token is None
            response = rest_client.get("/protected/posts/bearer-token")
            mock_obtain_token.assert_called_once()
            assert response.status_code == 200
            assert auth.access_token is not None
            expiry_0 = auth.token_expiry
            auth.token_expiry = auth.token_expiry.subtract(seconds=1)
            expiry_1 = auth.token_expiry
            assert expiry_0 > expiry_1
            assert auth.is_token_expired()

            response = rest_client.get("/protected/posts/bearer-token")
            assert mock_obtain_token.call_count == 2
            assert response.status_code == 200
            expiry_2 = auth.token_expiry
            assert expiry_2 > expiry_1
            assert response.json()["data"][0] == {"id": 0, "title": "Post 0"}

    def test_oauth_customized_token_request(self, rest_client: RESTClient):
        class OAuth2ClientCredentialsHTTPBasic(OAuth2ClientCredentials):
            """OAuth 2.0 as required by e.g. Zoom Video Communications, Inc."""

            def build_access_token_request(self) -> Dict[str, Any]:
                authentication: str = b64encode(
                    f"{self.client_id}:{self.client_secret}".encode()
                ).decode()
                return {
                    "headers": {
                        "Authorization": f"Basic {authentication}",
                        "Content-Type": "application/x-www-form-urlencoded",
                    },
                    "data": {
                        "grant_type": "account_credentials",
                        **self.access_token_request_data,
                    },
                }

        auth = OAuth2ClientCredentialsHTTPBasic(
            access_token_url=cast(TSecretStrValue, "https://api.example.com/custom-oauth/token"),
            client_id=cast(TSecretStrValue, "test-account-id"),
            client_secret=cast(TSecretStrValue, "test-client-secret"),
            access_token_request_data={
                "account_id": cast(TSecretStrValue, "test-account-id"),
            },
            session=Client().session,
        )

        assert auth.build_access_token_request() == {
            "headers": {
                "Authorization": "Basic dGVzdC1hY2NvdW50LWlkOnRlc3QtY2xpZW50LXNlY3JldA==",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            "data": {
                "grant_type": "account_credentials",
                "account_id": "test-account-id",
            },
        }

        rest_client.auth = auth
        pages_iter = rest_client.paginate("/protected/posts/bearer-token")

        assert_pagination(list(pages_iter))

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

    def test_paginate_json_body_without_params(self, rest_client) -> None:
        # leave 3 pages of data
        posts_skip = (DEFAULT_TOTAL_PAGES - 3) * DEFAULT_PAGE_SIZE

        class JSONBodyPageCursorPaginator(BaseReferencePaginator):
            def update_state(self, response, data):
                self._next_reference = response.json().get("next_page")

            def update_request(self, request):
                if request.json is None:
                    request.json = {}

                request.json["page"] = self._next_reference

        page_generator = rest_client.paginate(
            path="/posts/search",
            method="POST",
            json={"ids_greater_than": posts_skip - 1, "page_size": 5, "page_count": 5},
            paginator=JSONBodyPageCursorPaginator(),
        )
        result = [post for page in list(page_generator) for post in page]
        for i in range(3 * DEFAULT_PAGE_SIZE):
            assert result[i] == {"id": posts_skip + i, "title": f"Post {posts_skip + i}"}

    def test_post_json_body_without_params(self, rest_client) -> None:
        # leave two pages of data
        posts_skip = (DEFAULT_TOTAL_PAGES - 2) * DEFAULT_PAGE_SIZE
        result = rest_client.post(
            path="/posts/search",
            json={"ids_greater_than": posts_skip - 1},
        )
        returned_posts = result.json()["data"]
        assert len(returned_posts) == DEFAULT_PAGE_SIZE  # only one page is returned
        for i in range(DEFAULT_PAGE_SIZE):
            assert returned_posts[i] == {"id": posts_skip + i, "title": f"Post {posts_skip + i}"}

    def test_configurable_timeout(self, mocker) -> None:
        cfg = {
            "RUNTIME__REQUEST_TIMEOUT": 42,
        }
        os.environ.update({key: str(value) for key, value in cfg.items()})

        rest_client = RESTClient(
            base_url="https://api.example.com",
            session=Client().session,
        )

        import requests

        mocked_send = mocker.patch.object(requests.Session, "send")
        rest_client.get("/posts/1")
        assert mocked_send.call_args[1] == {
            "timeout": 42,
            "proxies": ANY,
            "stream": ANY,
            "verify": ANY,
            "cert": ANY,
        }

    def test_request_kwargs(self, mocker) -> None:
        rest_client = RESTClient(
            base_url="https://api.example.com",
            session=Client().session,
        )
        mocked_send = mocker.spy(rest_client.session, "send")

        rest_client.get(
            path="/posts/1",
            proxies={
                "http": "http://10.10.1.10:1111",
                "https": "http://10.10.1.10:2222",
            },
            stream=True,
            verify=False,
            cert=("/path/client.cert", "/path/client.key"),
            timeout=321,
            allow_redirects=False,
        )

        assert mocked_send.call_args[1] == {
            "proxies": {
                "http": "http://10.10.1.10:1111",
                "https": "http://10.10.1.10:2222",
            },
            "stream": True,
            "verify": False,
            "cert": ("/path/client.cert", "/path/client.key"),
            "timeout": 321,
            "allow_redirects": False,
        }

        next(
            rest_client.paginate(
                path="posts",
                proxies={
                    "http": "http://10.10.1.10:1234",
                    "https": "http://10.10.1.10:4321",
                },
                stream=True,
                verify=False,
                cert=("/path/client_2.cert", "/path/client_2.key"),
                timeout=432,
                allow_redirects=False,
            )
        )

        assert mocked_send.call_args[1] == {
            "proxies": {
                "http": "http://10.10.1.10:1234",
                "https": "http://10.10.1.10:4321",
            },
            "stream": True,
            "verify": False,
            "cert": ("/path/client_2.cert", "/path/client_2.key"),
            "timeout": 432,
            "allow_redirects": False,
        }
