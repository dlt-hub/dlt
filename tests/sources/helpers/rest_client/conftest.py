import re
from typing import NamedTuple, Callable, Pattern, Union, TYPE_CHECKING, Dict, List, Any
import base64

from urllib.parse import parse_qs, urlsplit, urlunsplit

import pytest
import requests_mock

from dlt.common import json

if TYPE_CHECKING:
    RequestCallback = Callable[
        [requests_mock.Request, requests_mock.Context], Union[str, Dict[str, Any], List[Any]]
    ]
    ResponseSerializer = Callable[[requests_mock.Request, requests_mock.Context], str]
else:
    RequestCallback = Callable
    ResponseSerializer = Callable

MOCK_BASE_URL = "https://api.example.com"


class Route(NamedTuple):
    method: str
    pattern: Pattern[str]
    callback: ResponseSerializer


class APIRouter:
    def __init__(self, base_url: str):
        self.routes: List[Route] = []
        self.base_url = base_url

    def _add_route(self, method: str, pattern: str, func: RequestCallback) -> RequestCallback:
        compiled_pattern = re.compile(f"{self.base_url}{pattern}")

        def serialize_response(request, context):
            result = func(request, context)

            if isinstance(result, dict) or isinstance(result, list):
                return json.dumps(result)

            return result

        self.routes.append(Route(method, compiled_pattern, serialize_response))
        return serialize_response

    def get(self, pattern: str) -> Callable[[RequestCallback], RequestCallback]:
        def decorator(func: RequestCallback) -> RequestCallback:
            return self._add_route("GET", pattern, func)

        return decorator

    def post(self, pattern: str) -> Callable[[RequestCallback], RequestCallback]:
        def decorator(func: RequestCallback) -> RequestCallback:
            return self._add_route("POST", pattern, func)

        return decorator

    def register_routes(self, mocker: requests_mock.Mocker) -> None:
        for route in self.routes:
            mocker.register_uri(
                route.method,
                route.pattern,
                text=route.callback,
            )


router = APIRouter(MOCK_BASE_URL)


def serialize_page(
    records,
    page_number,
    total_pages,
    request_url,
    records_key="data",
    use_absolute_url=True,
):
    """Serialize a page of records into a dict with pagination metadata."""
    if records_key is None:
        return records

    response = {
        records_key: records,
        "page": page_number,
        "total_pages": total_pages,
    }

    if page_number < total_pages:
        next_page = page_number + 1

        scheme, netloc, path, _, _ = urlsplit(request_url)
        if use_absolute_url:
            next_page_url = urlunsplit([scheme, netloc, path, f"page={next_page}", ""])
        else:
            next_page_url = f"{path}?page={next_page}"

        response["next_page"] = next_page_url

    return response


def generate_posts(count=100):
    return [{"id": i, "title": f"Post {i}"} for i in range(count)]


def generate_comments(post_id, count=50):
    return [{"id": i, "body": f"Comment {i} for post {post_id}"} for i in range(count)]


def get_page_number(qs, key="page", default=1):
    return int(qs.get(key, [default])[0])


def paginate_response(request, records, page_size=10, records_key="data", use_absolute_url=True):
    page_number = get_page_number(request.qs)
    total_records = len(records)
    total_pages = (total_records + page_size - 1) // page_size
    start_index = (page_number - 1) * 10
    end_index = start_index + 10
    records_slice = records[start_index:end_index]
    return serialize_page(
        records_slice,
        page_number,
        total_pages,
        request.url,
        records_key,
        use_absolute_url,
    )


@pytest.fixture(scope="module")
def mock_api_server():
    with requests_mock.Mocker() as m:

        @router.get(r"/posts_no_key(\?page=\d+)?$")
        def posts_no_key(request, context):
            return paginate_response(request, generate_posts(), records_key=None)

        @router.get(r"/posts(\?page=\d+)?$")
        def posts(request, context):
            return paginate_response(request, generate_posts())

        @router.get(r"/posts_relative_next_url(\?page=\d+)?$")
        def posts_relative_next_url(request, context):
            return paginate_response(request, generate_posts(), use_absolute_url=False)

        @router.get(r"/posts/(\d+)/comments")
        def post_comments(request, context):
            post_id = int(request.url.split("/")[-2])
            return paginate_response(request, generate_comments(post_id))

        @router.get(r"/posts/\d+$")
        def post_detail(request, context):
            post_id = request.url.split("/")[-1]
            return {"id": post_id, "body": f"Post body {post_id}"}

        @router.get(r"/posts/\d+/some_details_404")
        def post_detail_404(request, context):
            """Return 404 for post with id > 0. Used to test ignoring 404 errors."""
            post_id = int(request.url.split("/")[-2])
            if post_id < 1:
                return {"id": post_id, "body": f"Post body {post_id}"}
            else:
                context.status_code = 404
                return {"error": "Post not found"}

        @router.get(r"/posts_under_a_different_key$")
        def posts_with_results_key(request, context):
            return paginate_response(request, generate_posts(), records_key="many-results")

        @router.get("/protected/posts/basic-auth")
        def protected_basic_auth(request, context):
            auth = request.headers.get("Authorization")
            creds = "user:password"
            creds_base64 = base64.b64encode(creds.encode()).decode()
            if auth == f"Basic {creds_base64}":
                return paginate_response(request, generate_posts())
            context.status_code = 401
            return {"error": "Unauthorized"}

        @router.get("/protected/posts/bearer-token")
        def protected_bearer_token(request, context):
            auth = request.headers.get("Authorization")
            if auth == "Bearer test-token":
                return paginate_response(request, generate_posts())
            context.status_code = 401
            return {"error": "Unauthorized"}

        @router.get("/protected/posts/bearer-token-plain-text-error")
        def protected_bearer_token_plain_text_erorr(request, context):
            auth = request.headers.get("Authorization")
            if auth == "Bearer test-token":
                return paginate_response(request, generate_posts())
            context.status_code = 401
            return "Unauthorized"

        @router.get("/protected/posts/api-key")
        def protected_api_key(request, context):
            api_key = request.headers.get("x-api-key")
            if api_key == "test-api-key":
                return paginate_response(request, generate_posts())
            context.status_code = 401
            return {"error": "Unauthorized"}

        @router.post("/oauth/token")
        def oauth_token(request, context):
            if oauth_authorize(request):
                return {"access_token": "test-token", "expires_in": 3600}
            context.status_code = 401
            return {"error": "Unauthorized"}

        @router.post("/oauth/token-expires-now")
        def oauth_token_expires_now(request, context):
            if oauth_authorize(request):
                return {"access_token": "test-token", "expires_in": 0}
            context.status_code = 401
            return {"error": "Unauthorized"}

        @router.post("/auth/refresh")
        def refresh_token(request, context):
            body = request.json()
            if body.get("refresh_token") == "valid-refresh-token":
                return {"access_token": "new-valid-token"}
            context.status_code = 401
            return {"error": "Invalid refresh token"}

        @router.post("/custom-oauth/token")
        def custom_oauth_token(request, context):
            qs = parse_qs(request.text)
            if (
                qs.get("grant_type")[0] == "account_credentials"
                and qs.get("account_id")[0] == "test-account-id"
                and request.headers["Authorization"]
                == "Basic dGVzdC1hY2NvdW50LWlkOnRlc3QtY2xpZW50LXNlY3JldA=="
            ):
                return {"access_token": "test-token", "expires_in": 3600}
            context.status_code = 401
            return {"error": "Unauthorized"}

        router.register_routes(m)

        yield m


def oauth_authorize(request):
    qs = parse_qs(request.text)
    grant_type = qs.get("grant_type")[0]
    if "jwt-bearer" in grant_type:
        return True
    if "client_credentials" in grant_type:
        return (
            qs["client_secret"][0] == "test-client-secret"
            and qs["client_id"][0] == "test-client-id"
        )


def assert_pagination(pages, expected_start=0, page_size=10, total_pages=10):
    assert len(pages) == total_pages
    for i, page in enumerate(pages):
        assert page == [{"id": i, "title": f"Post {i}"} for i in range(i * 10, (i + 1) * 10)]
