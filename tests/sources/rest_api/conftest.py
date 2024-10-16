import base64
from urllib.parse import parse_qs, urlsplit, urlunsplit, urlencode

import pytest
import requests_mock

from dlt.sources.helpers.rest_client import RESTClient

from tests.sources.helpers.rest_client.api_router import APIRouter
from tests.sources.helpers.rest_client.paginators import (
    PageNumberPaginator,
    OffsetPaginator,
    CursorPaginator,
)


MOCK_BASE_URL = "https://api.example.com"
DEFAULT_PAGE_SIZE = 5
DEFAULT_TOTAL_PAGES = 5
DEFAULT_LIMIT = 10


router = APIRouter(MOCK_BASE_URL)


def generate_posts(count=DEFAULT_PAGE_SIZE * DEFAULT_TOTAL_PAGES):
    return [{"id": i, "title": f"Post {i}"} for i in range(count)]


def generate_comments(post_id, count=50):
    return [
        {"id": i, "post_id": post_id, "body": f"Comment {i} for post {post_id}"}
        for i in range(count)
    ]


def get_page_number(qs, key="page", default=1):
    return int(qs.get(key, [default])[0])


def create_next_page_url(request, paginator, use_absolute_url=True):
    scheme, netloc, path, _, _ = urlsplit(request.url)
    query = urlencode(paginator.next_page_url_params)
    if use_absolute_url:
        return urlunsplit([scheme, netloc, path, query, ""])
    else:
        return f"{path}?{query}"


def paginate_by_page_number(
    request, records, records_key="data", use_absolute_url=True, index_base=1
):
    page_number = get_page_number(request.qs, default=index_base)
    paginator = PageNumberPaginator(records, page_number, index_base=index_base)

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

        @router.get(r"/posts_no_key(\?page=\d+)?$")
        def posts_no_key(request, context):
            return paginate_by_page_number(request, generate_posts(), records_key=None)

        @router.get(r"/posts(\?page=\d+)?$")
        def posts(request, context):
            return paginate_by_page_number(request, generate_posts())

        @router.get(r"/posts_zero_based(\?page=\d+)?$")
        def posts_zero_based(request, context):
            return paginate_by_page_number(request, generate_posts(), index_base=0)

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
            limit = int(request.qs.get("limit", [DEFAULT_LIMIT])[0])
            paginator = OffsetPaginator(records, offset, limit)

            return {
                "data": paginator.page_records,
                **paginator.metadata,
            }

        @router.get(r"/posts_cursor(\?cursor=\d+)?$")
        def posts_cursor(request, context):
            records = generate_posts()
            cursor = int(request.qs.get("cursor", [0])[0])
            paginator = CursorPaginator(records, cursor)

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
            return {"id": int(post_id), "body": f"Post body {post_id}"}

        @router.get(r"/posts/\d+/some_details_404")
        def post_detail_404(request, context):
            """Return 404 for post with id > 0. Used to test ignoring 404 errors."""
            post_id = int(request.url.split("/")[-2])
            if post_id < 1:
                return {"id": post_id, "body": f"Post body {post_id}"}
            else:
                context.status_code = 404
                return {"error": f"Post with id {post_id} not found"}

        @router.get(r"/posts/\d+/some_details_404_others_422")
        def post_detail_404_422(request, context):
            """Return 404  No Content for post with id 1. Return 422 for post with id > 1.
            Used to test ignoring 404 and 422 responses."""
            post_id = int(request.url.split("/")[-2])
            if post_id < 1:
                return {"id": post_id, "body": f"Post body {post_id}"}
            elif post_id == 1:
                context.status_code = 404
                return {"error": f"Post with id {post_id} not found"}
            else:
                context.status_code = 422
                return None

        @router.get(r"/posts/\d+/some_details_204")
        def post_detail_204(request, context):
            """Return 204  No Content for post with id > 0. Used to test ignoring 204 responses."""
            post_id = int(request.url.split("/")[-2])
            if post_id < 1:
                return {"id": post_id, "body": f"Post body {post_id}"}
            else:
                context.status_code = 204
                return None

        @router.get(r"/posts_under_a_different_key$")
        def posts_with_results_key(request, context):
            return paginate_by_page_number(request, generate_posts(), records_key="many-results")

        @router.post(r"/posts/search$")
        def search_posts(request, context):
            body = request.json()
            page_size = body.get("page_size", DEFAULT_PAGE_SIZE)
            page_count = body.get("page_count", DEFAULT_TOTAL_PAGES)
            page_number = body.get("page", 1)

            # Simulate a search with filtering
            records = generate_posts(page_size * page_count)
            ids_greater_than = body.get("ids_greater_than", 0)
            records = [r for r in records if r["id"] > ids_greater_than]

            total_records = len(records)
            total_pages = (total_records + page_size - 1) // page_size
            start_index = (page_number - 1) * page_size
            end_index = start_index + page_size
            records_slice = records[start_index:end_index]

            return {
                "data": records_slice,
                "next_page": page_number + 1 if page_number < total_pages else None,
            }

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


@pytest.fixture
def rest_client() -> RESTClient:
    return RESTClient(
        base_url="https://api.example.com",
        headers={"Accept": "application/json"},
    )


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


def assert_pagination(pages, page_size=DEFAULT_PAGE_SIZE, total_pages=DEFAULT_TOTAL_PAGES):
    assert len(pages) == total_pages
    for i, page in enumerate(pages):
        assert page == [
            {"id": i, "title": f"Post {i}"} for i in range(i * page_size, (i + 1) * page_size)
        ]
