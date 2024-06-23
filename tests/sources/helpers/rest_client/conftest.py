import base64

from urllib.parse import urlsplit, urlunsplit, urlencode

import pytest
import requests_mock

from dlt.sources.helpers.rest_client import RESTClient

from .api_router import APIRouter
from .paginators import PageNumberPaginator, OffsetPaginator


MOCK_BASE_URL = "https://api.example.com"


router = APIRouter(MOCK_BASE_URL)


def generate_posts(count=100):
    return [{"id": i, "title": f"Post {i}"} for i in range(count)]


def generate_comments(post_id, count=50):
    return [{"id": i, "body": f"Comment {i} for post {post_id}"} for i in range(count)]


def get_page_number(qs, key="page", default=1):
    return int(qs.get(key, [default])[0])


def create_next_page_url(request, paginator, use_absolute_url=True):
    scheme, netloc, path, _, _ = urlsplit(request.url)
    query = urlencode(paginator.next_page_url_params)
    if use_absolute_url:
        return urlunsplit([scheme, netloc, path, query, ""])
    else:
        return f"{path}?{query}"


def paginate_by_page_number(request, records, records_key="data", use_absolute_url=True):
    page_number = get_page_number(request.qs)
    paginator = PageNumberPaginator(records, page_number)

    response = {
        records_key: paginator.page_records,
        **paginator.metadata,
    }

    if paginator.next_page_url_params:
        response["next_page"] = create_next_page_url(request, paginator, use_absolute_url)

    return response


@pytest.fixture(scope="module")
def mock_api_server():
    with requests_mock.Mocker() as m:

        @router.get(r"/posts(\?page=\d+)?$")
        def posts(request, context):
            return paginate_by_page_number(request, generate_posts())

        @router.get(r"/posts_header_link(\?page=\d+)?$")
        def posts_header_link(request, context):
            records = generate_posts()
            page_number = get_page_number(request.qs)
            paginator = PageNumberPaginator(records, page_number)

            response = paginator.page_records

            if paginator.next_page_url_params:
                next_page_url = create_next_page_url(request, paginator)
                context.headers["Link"] = f'<{next_page_url}>; rel="next"'

            return response

        @router.get(r"/posts_relative_next_url(\?page=\d+)?$")
        def posts_relative_next_url(request, context):
            return paginate_by_page_number(request, generate_posts(), use_absolute_url=False)

        @router.get(r"/posts_offset_limit(\?offset=\d+&limit=\d+)?$")
        def posts_offset_limit(request, context):
            records = generate_posts()
            offset = int(request.qs.get("offset", [0])[0])
            limit = int(request.qs.get("limit", [10])[0])
            paginator = OffsetPaginator(records, offset, limit)

            return {
                "data": paginator.page_records,
                **paginator.metadata,
            }

        @router.get(r"/posts/(\d+)/comments")
        def post_comments(request, context):
            post_id = int(request.url.split("/")[-2])
            return paginate_by_page_number(request, generate_comments(post_id))

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
            return paginate_by_page_number(request, generate_posts(), records_key="many-results")

        @router.get("/protected/posts/basic-auth")
        def protected_basic_auth(request, context):
            auth = request.headers.get("Authorization")
            creds = "user:password"
            creds_base64 = base64.b64encode(creds.encode()).decode()
            if auth == f"Basic {creds_base64}":
                return paginate_by_page_number(request, generate_posts())
            context.status_code = 401
            return {"error": "Unauthorized"}

        @router.get("/protected/posts/bearer-token")
        def protected_bearer_token(request, context):
            auth = request.headers.get("Authorization")
            if auth == "Bearer test-token":
                return paginate_by_page_number(request, generate_posts())
            context.status_code = 401
            return {"error": "Unauthorized"}

        @router.get("/protected/posts/bearer-token-plain-text-error")
        def protected_bearer_token_plain_text_erorr(request, context):
            auth = request.headers.get("Authorization")
            if auth == "Bearer test-token":
                return paginate_by_page_number(request, generate_posts())
            context.status_code = 401
            return "Unauthorized"

        @router.get("/protected/posts/api-key")
        def protected_api_key(request, context):
            api_key = request.headers.get("x-api-key")
            if api_key == "test-api-key":
                return paginate_by_page_number(request, generate_posts())
            context.status_code = 401
            return {"error": "Unauthorized"}

        @router.post("/oauth/token")
        def oauth_token(request, context):
            return {"access_token": "test-token", "expires_in": 3600}

        @router.post("/auth/refresh")
        def refresh_token(request, context):
            body = request.json()
            if body.get("refresh_token") == "valid-refresh-token":
                return {"access_token": "new-valid-token"}
            context.status_code = 401
            return {"error": "Invalid refresh token"}

        router.register_routes(m)

        yield m


@pytest.fixture
def rest_client() -> RESTClient:
    return RESTClient(
        base_url="https://api.example.com",
        headers={"Accept": "application/json"},
    )


def assert_pagination(pages, expected_start=0, page_size=10, total_pages=10):
    assert len(pages) == total_pages
    for i, page in enumerate(pages):
        assert page == [{"id": i, "title": f"Post {i}"} for i in range(i * 10, (i + 1) * 10)]
