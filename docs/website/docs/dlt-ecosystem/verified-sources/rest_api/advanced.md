---
title: REST API helpers
description: Use the dlt RESTClient to interact with RESTful APIs and paginate the results
keywords: [api, http, rest, restful, requests, restclient, paginate, pagination, json, retry, timeout, headers, response actions, advanced configuration]
---

dlt has built-in support for fetching data from APIs:
- RESTClient for interacting with RESTful APIs and paginating the results
- Requests wrapper for making simple HTTP requests with automatic retries and timeouts

Additionally, dlt provides tools to simplify working with APIs:
- [REST API generic source](../../dlt-ecosystem/verified-sources/rest_api) integrates APIs using a [declarative configuration](../../dlt-ecosystem/verified-sources/rest_api/basic#source-configuration) to minimize custom code.
- [OpenAPI source generator](../../dlt-ecosystem/verified-sources/openapi-generator) automatically creates declarative API configurations from [OpenAPI specifications](https://swagger.io/specification/).

## Quick example

Here's a simple pipeline that reads issues from the [dlt GitHub repository](https://github.com/dlt-hub/dlt/issues). The API endpoint is https://api.github.com/repos/dlt-hub/dlt/issues. The result is "paginated," meaning that the API returns a limited number of issues per page. The `paginate()` method iterates over all pages and yields the results which are then processed by the pipeline.

```py
import dlt
from dlt.sources.helpers.rest_client import RESTClient

github_client = RESTClient(base_url="https://api.github.com")  # (1)

@dlt.resource
def get_issues():
    for page in github_client.paginate(                        # (2)
        "/repos/dlt-hub/dlt/issues",                           # (3)
        params={                                               # (4)
            "per_page": 100,
            "sort": "updated",
            "direction": "desc",
        },
    ):
        yield page                                             # (5)


pipeline = dlt.pipeline(
    pipeline_name="github_issues",
    destination="duckdb",
    dataset_name="github_data",
)
load_info = pipeline.run(get_issues)
print(load_info)
```

Here's what the code does:
1. We create a `RESTClient` instance with the base URL of the API: in this case, the GitHub API (https://api.github.com).
2. The issues endpoint returns a list of issues. Since there could be hundreds of issues, the API "paginates" the results: it returns a limited number of issues in each response along with a link to the next batch of issues (or "page"). The `paginate()` method iterates over all pages and yields the batches of issues.
3. Here we specify the address of the endpoint we want to read from: `/repos/dlt-hub/dlt/issues`.
4. We pass the parameters to the actual API call to control the data we get back. In this case, we ask for 100 issues per page (`"per_page": 100`), sorted by the last update date (`"sort": "updated"`) in descending order (`"direction": "desc"`).
5. We yield the page from the resource function to the pipeline. The `page` is an instance of the [`PageData`](./rest-client#pagedata) and contains the data from the current page of the API response and some metadata.

Note that we do not explicitly specify the pagination parameters in the example. The `paginate()` method handles pagination automatically: it detects the pagination mechanism used by the API from the response. What if you need to specify the pagination method and parameters explicitly? Let's see how to do that in a different example below.

## Explicitly specifying pagination parameters

```py
import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator

github_client = RESTClient(
    base_url="https://pokeapi.co/api/v2",
    paginator=JSONLinkPaginator(next_url_path="next"),   # (1)
    data_selector="results",                             # (2)
)

@dlt.resource
def get_pokemons():
    for page in github_client.paginate(
        "/pokemon",
        params={
            "limit": 100,                                    # (3)
        },
    ):
        yield page

pipeline = dlt.pipeline(
    pipeline_name="get_pokemons",
    destination="duckdb",
    dataset_name="github_data",
)
load_info = pipeline.run(get_pokemons)
print(load_info)
```

In the example above:
1. We create a `RESTClient` instance with the base URL of the API: in this case, the [PokéAPI](https://pokeapi.co/). We also specify the paginator to use explicitly: `JSONLinkPaginator` with the `next_url_path` set to `"next"`. This tells the paginator to look for the next page URL in the `next` key of the JSON response.
2. In `data_selector`, we specify the JSON path to extract the data from the response. This is used to extract the data from the response JSON.
3. By default, the number of items per page is limited to 20. We override this by specifying the `limit` parameter in the API call.

## RESTClient

The `RESTClient` class offers an interface for interacting with RESTful APIs, including features like:
- automatic pagination,
- various authentication mechanisms,
- customizable request/response handling.

This guide shows how to use the `RESTClient` class to read data from APIs, focusing on the `paginate()` method for fetching data from paginated API responses.

## Creating a RESTClient instance

```py
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator

client = RESTClient(
    base_url="https://api.example.com",
    headers={"User-Agent": "MyApp/1.0"},
    auth=BearerTokenAuth(token="your_access_token_here"),  # type: ignore
    paginator=JSONLinkPaginator(next_url_path="pagination.next"),
    data_selector="data",
    session=MyCustomSession()
)
```

The `RESTClient` class is initialized with the following parameters:

- `base_url`: The root URL of the API. All requests will be made relative to this URL.
- `headers`: Default headers to include in every request. This can be used to set common headers like `User-Agent` or other custom headers.
- `auth`: The authentication configuration. See the [Authentication](#authentication) section for more details.
- `paginator`: A paginator instance for handling paginated responses. See the [Paginators](#paginators) section below.
- `data_selector`: A [JSONPath selector](https://github.com/h2non/jsonpath-ng?tab=readme-ov-file#jsonpath-syntax) for extracting data from the responses. This defines a way to extract the data from the response JSON. Only used when paginating.
- `session`: An optional session for making requests. This should be a [Requests session](https://requests.readthedocs.io/en/latest/api/#requests.Session) instance that can be used to set up custom request behavior for the client.

## Making basic requests

To perform basic GET and POST requests, use the `get()` and `post()` methods respectively. This is similar to how the `requests` library works:

```py
client = RESTClient(base_url="https://api.example.com")
response = client.get("/posts/1")
```

### POST requests with data

The `post()` method supports both JSON payloads and form-encoded/raw data through separate parameters:

```py
# JSON payload (sets Content-Type: application/json)
response = client.post("/posts", json={"title": "New post", "content": "Post content"})

# Form-encoded data (sets Content-Type: application/x-www-form-urlencoded)
response = client.post("/posts", data={"field1": "value1", "field2": "value2"})

# Raw string or bytes data
response = client.post("/posts", data="raw text data")
```

:::note
The `json` and `data` parameters are mutually exclusive. You cannot use both in the same request. Use `json` for JSON payloads or `data` for form-encoded/raw data.
:::

## Paginating API responses

The `RESTClient.paginate()` method is specifically designed to handle paginated responses, yielding `PageData` instances for each page:

```py
for page in client.paginate("/posts"):
    print(page)
```

The `paginate()` method supports the same request parameters as regular requests:

```py
# Paginating with JSON payload
for page in client.paginate("/search", method="POST", json={"query": "python"}):
    print(page)

# Paginating with form-encoded data
for page in client.paginate("/search", method="POST", data={"q": "python", "limit": 10}):
    print(page)
```

:::tip
If a `paginator` is not specified, the `paginate()` method will attempt to automatically detect the pagination mechanism used by the API. If the API uses a standard pagination mechanism like having a `next` link in the response's headers or JSON body, the `paginate()` method will handle this automatically. Otherwise, you can specify a paginator object explicitly or implement a custom paginator.
:::

### Selecting data from the response

When paginating through API responses, the `RESTClient` tries to automatically extract the data from the response. Sometimes, however, you may need to explicitly specify how to extract the data from the response JSON.

Use the `data_selector` parameter of the `RESTClient` class or the `paginate()` method to tell the client how to extract the data.
`data_selector` is a [JSONPath](https://github.com/h2non/jsonpath-ng?tab=readme-ov-file#jsonpath-syntax) expression that points to the key in the JSON that contains the data to be extracted.

For example, if the API response looks like this:

```json
{
    "posts": [
        {"id": 1, "title": "Post 1"},
        {"id": 2, "title": "Post 2"},
        {"id": 3, "title": "Post 3"}
    ]
}
```

The `data_selector` should be set to `"posts"` to extract the list of posts from the response.

For a nested structure like this:

```json
{
    "results": {
        "posts": [
            {"id": 1, "title": "Post 1"},
            {"id": 2, "title": "Post 2"},
            {"id": 3, "title": "Post 3"}
        ]
    }
}
```

The `data_selector` needs to be set to `"results.posts"`. Read more about [JSONPath syntax](https://github.com/h2non/jsonpath-ng?tab=readme-ov-file#jsonpath-syntax) to learn how to write selectors.

### PageData

Each `PageData` instance contains the data for a single page, along with context such as the original request and response objects, allowing for detailed inspection. The `PageData` is a list-like object that contains the following attributes:

- `request`: The original request object.
- `response`: The response object.
- `paginator`: The paginator object used to paginate the response.
- `auth`: The authentication object used for the request.

### Paginators

Paginators are used to handle paginated responses. The `RESTClient` class comes with built-in paginators for common pagination mechanisms:

- [JSONLinkPaginator](#jsonlinkpaginator) - link to the next page is included in the JSON response.
- [HeaderLinkPaginator](#headerlinkpaginator) - link to the next page is included in the response headers.
- [OffsetPaginator](#offsetpaginator) - pagination based on offset and limit query parameters.
- [PageNumberPaginator](#pagenumberpaginator) - pagination based on page numbers.
- [JSONResponseCursorPaginator](#jsonresponsecursorpaginator) - pagination based on a cursor in the JSON response.
- [HeaderCursorPaginator](#headercursorpaginator) - pagination based on a cursor in the response headers.

If the API uses a non-standard pagination, you can [implement a custom paginator](#implementing-a-custom-paginator) by subclassing the `BasePaginator` class.

#### JSONLinkPaginator

`JSONLinkPaginator` is designed for APIs where the next page URL is included in the response's JSON body. This paginator uses a JSONPath to locate the next page URL within the JSON response.

**Parameters:**

- `next_url_path`: A JSONPath string pointing to the key in the JSON response that contains the next page URL.

**Example:**

Suppose the API response for `https://api.example.com/posts` looks like this:

```json
{
    "data": [
        {"id": 1, "title": "Post 1"},
        {"id": 2, "title": "Post 2"},
        {"id": 3, "title": "Post 3"}
    ],
    "pagination": {
        "next": "https://api.example.com/posts?page=2"
    }
}
```

To paginate this response, you can use the `JSONLinkPaginator` with the `next_url_path` set to `"pagination.next"`:

```py
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator

client = RESTClient(
    base_url="https://api.example.com",
    paginator=JSONLinkPaginator(next_url_path="pagination.next")
)

@dlt.resource
def get_data():
    for page in client.paginate("/posts"):
        yield page
```

#### HeaderLinkPaginator

This paginator handles pagination based on a link to the next page in the response headers (e.g., the `Link` header, as used by the GitHub API).

**Parameters:**

- `links_next_key`: The relation type (rel) to identify the next page link within the Link header. Defaults to "next".

Note: Normally, you don't need to specify this paginator explicitly, as it is used automatically when the API returns a `Link` header. On rare occasions, you may need to specify the paginator when the API uses a different relation type.

#### OffsetPaginator

`OffsetPaginator` handles pagination based on an offset and limit in the query parameters.

**Parameters:**

- `limit`: The maximum number of items to retrieve in each request.
- `offset`: The initial offset for the first request. Defaults to `0`.
- `offset_param`: The name of the query parameter used to specify the offset. Defaults to `"offset"`.
- `offset_body_path`: A JSONPath expression specifying where to place the offset in the request JSON body. If provided, the paginator will use this instead of `offset_param` to send the offset in the request body. Defaults to `None`.
- `limit_param`: The name of the query parameter used to specify the limit. Defaults to `"limit"`.
- `limit_body_path`: A JSONPath expression specifying where to place the limit in the request JSON body. If provided, the paginator will use this instead of `limit_param` to send the limit in the request body. Defaults to `None`.
- `total_path`: A JSONPath expression for the total number of items. If not provided, pagination is controlled by `maximum_offset` and `stop_after_empty_page`.
- `maximum_offset`: Optional maximum offset value. Limits pagination even without a total count.
- `stop_after_empty_page`: Whether pagination should stop when a page contains no result items. Defaults to `True`.
- `has_more_path`: A JSONPath expression for a boolean indicator of whether additional pages exist. Defaults to `None`.

**Example:**

Assuming an API endpoint `https://api.example.com/items` supports pagination with `offset` and `limit` parameters.
E.g., `https://api.example.com/items?offset=0&limit=100`, `https://api.example.com/items?offset=100&limit=100`, etc., and includes the total count in its responses, e.g.:

```json
{
  "items": ["one", "two", "three"],
  "total": 1000
}
```

You can paginate through responses from this API using the `OffsetPaginator`:

```py
client = RESTClient(
    base_url="https://api.example.com",
    paginator=OffsetPaginator(
        limit=100,
        total_path="total"
    )
)
```

Pagination stops by default when a page contains no records. This is especially useful when the API does not provide the total item count.
Here, the `total_path` parameter is set to `None` because the API does not provide the total count.

```py
client = RESTClient(
    base_url="https://api.example.com",
    paginator=OffsetPaginator(
        limit=100,
        total_path=None,
    )
)
```

Additionally, you can limit pagination with `maximum_offset`, for example during development. If `maximum_offset` is reached before the first empty page, then pagination stops:

```py
client = RESTClient(
    base_url="https://api.example.com",
    paginator=OffsetPaginator(
        limit=10,
        maximum_offset=20,  # limits response to 20 records
        total_path=None,
    )
)
```

If the API provides a boolean flag when all pages have been returned, you can use `has_more_path` to recognize this indicator and end pagination:

```py
client = RESTClient(
    base_url="https://api.example.com",
    paginator=OffsetPaginator(
        limit=10,
        total_path=None,
        has_more_path="has_more",
    )
)
```

You can disable automatic stoppage of pagination by setting `stop_after_empty_page = False`. In this case, you must provide either `total_path`, `maximum_offset`, or `has_more_path` to guarantee that the paginator terminates.

#### PageNumberPaginator

`PageNumberPaginator` works by incrementing the page number for each request.

**Parameters:**

- `base_page`: The index of the initial page from the API perspective. Normally, it's 0-based or 1-based (e.g., 1, 2, 3, ...) indexing for the pages. Defaults to 0.
- `page`: The page number for the first request. If not provided, the initial value will be set to `base_page`.
- `page_param`: The query parameter name for the page number. Defaults to `"page"`.
- `page_body_path`: A JSONPath expression specifying where to place the page number in the request JSON body. Use this instead of `page_param` when sending the page number in the request body. Defaults to `None`.
- `total_path`: A JSONPath expression for the total number of pages. If not provided, pagination is controlled by `maximum_page` and `stop_after_empty_page`.
- `maximum_page`: Optional maximum page number. Stops pagination once this page is reached.
- `stop_after_empty_page`: Whether pagination should stop when a page contains no result items. Defaults to `True`.
- `has_more_path`: A JSONPath expression for a boolean indicator of whether additional pages exist. Defaults to `None`.

**Example:**

Assuming an API endpoint `https://api.example.com/items` paginates by page numbers and provides a total page count in its responses, e.g.:

```json
{
  "items": ["one", "two", "three"],
  "total_pages": 10
}
```

You can paginate through responses from this API using the `PageNumberPaginator`:

```py
client = RESTClient(
    base_url="https://api.example.com",
    paginator=PageNumberPaginator(
        total_path="total_pages"  # Uses the total page count from the API
    )
)
```

Pagination stops by default when a page contains no records. This is especially useful when the API does not provide the total item count.
Here, the `total_path` parameter is set to `None` because the API does not provide the total count.

```py
client = RESTClient(
    base_url="https://api.example.com",
    paginator=PageNumberPaginator(
        total_path=None
    )
)
```

Additionally, you can limit pagination with `maximum_page`, for example during development. If `maximum_page` is reached before the first empty page, then pagination stops:

```py
client = RESTClient(
    base_url="https://api.example.com",
    paginator=PageNumberPaginator(
        maximum_page=2,  # Limits response to 2 pages
        total_path=None
    )
)
```

If the API provides a boolean flag when all pages have been returned, you can use `has_more_path` to recognize this indicator and end pagination:

```py
client = RESTClient(
    base_url="https://api.example.com",
    paginator=PageNumberPaginator(
        total_path=None,
        has_more_path="has_more",
    )
)
```

You can disable automatic stoppage of pagination by setting `stop_after_empty_page = False`. In this case, you must provide `total_path`, `has_more_path`, or `maximum_page` to guarantee that the paginator terminates.

#### JSONResponseCursorPaginator

`JSONResponseCursorPaginator` handles pagination based on a cursor in the JSON response.

**Parameters:**

- `cursor_path`: A JSONPath expression pointing to the cursor in the JSON response. This cursor is used to fetch subsequent pages. Defaults to `"cursors.next"`.
- `cursor_param`: The query parameter used to send the cursor value in the next request. Defaults to `"cursor"` if neither `cursor_param` nor `cursor_body_path` is provided.
- `cursor_body_path`: A JSONPath expression specifying where to place the cursor in the request JSON body. Use this instead of `cursor_param` when sending the cursor in the request body.

Note: You must provide either `cursor_param` or `cursor_body_path`, but not both. If neither is provided, `cursor_param` will default to `"cursor"`.

**Example:**

Consider an API endpoint `https://api.example.com/data` returning a structure where a cursor to the next page is included in the response:

```json
{
  "items": ["one", "two", "three"],
  "cursors": {
    "next": "cursor_string_for_next_page"
  }
}
```

To paginate through responses from this API using GET requests with query parameters, use `JSONResponseCursorPaginator` with `cursor_path` and `cursor_param`:

```py
client = RESTClient(
    base_url="https://api.example.com",
    paginator=JSONResponseCursorPaginator(
        cursor_path="cursors.next",
        cursor_param="cursor"
    )
)
```

For requests with a JSON body, you can specify where to place the cursor in the request body using the `cursor_body_path` parameter:

```py
client = RESTClient(
    base_url="https://api.example.com",
    paginator=JSONResponseCursorPaginator(
        cursor_path="nextPageToken",
        cursor_body_path="nextPageToken"  # Adds cursor to root of JSON body
    )
)

# For nested placement in JSON body
client = RESTClient(
    base_url="https://api.example.com",
    paginator=JSONResponseCursorPaginator(
        cursor_path="meta.nextToken",
        cursor_body_path="pagination.cursor"  # Will create {"pagination": {"cursor": "token_value"}}
    )
)

@dlt.resource
def get_data():
    for page in client.paginate("/search", method="POST", json={"query": "example"}):
        yield page
```

#### HeaderCursorPaginator

`HeaderCursorPaginator` handles pagination based on a cursor in the response headers.

**Parameters:**

- `cursor_key`: The key in the response headers that contains the cursor value. Defaults to `"next"`.
- `cursor_param`: The query parameter used to send the cursor value in the next request. Defaults to `"cursor"`.

**Example:**

Consider an API endpoint `https://api.example.com/items` returning a response with a `NextPageToken` header containing the cursor for the next page:

```text
Content-Type: application/json
NextPageToken: n3xtp4g3

[
    {"id": 1, "name": "item1"},
    {"id": 2, "name": "item2"},
    ...
]
```

To paginate through responses from this API, use `HeaderCursorPaginator` with `cursor_key` set to `"NextPageToken"`:

```py
client = RESTClient(
    base_url="https://api.example.com",
    paginator=HeaderCursorPaginator(cursor_key="NextPageToken")
)
```

### Implementing a custom paginator

When working with APIs that use non-standard pagination schemes, or when you need more control over the pagination process, you can implement a custom paginator by subclassing the `BasePaginator` class and implementing the methods `init_request`, `update_state`, and `update_request`.

- `init_request(request: Request) -> None`: This method is called before making the first API call in the `RESTClient.paginate` method. You can use this method to set up the initial request query parameters, headers, etc. For example, you can set the initial page number or cursor value.

- `update_state(response: Response, data: Optional[List[Any]]) -> None`: This method updates the paginator's state based on the response of the API call. Typically, you extract pagination details (like the next page reference) from the response and store them in the paginator instance.

- `update_request(request: Request) -> None`: Before making the next API call in the `RESTClient.paginate` method, `update_request` is used to modify the request with the necessary parameters to fetch the next page (based on the current state of the paginator). For example, you can add query parameters to the request or modify the URL.

#### Example 1: Creating a query parameter paginator

Suppose an API uses query parameters for pagination, incrementing a page parameter for each subsequent page, without providing direct links to the next pages in its responses. E.g., `https://api.example.com/posts?page=1`, `https://api.example.com/posts?page=2`, etc. Here's how you could implement a paginator for this scheme:

```py
from typing import Any, List, Optional
from dlt.sources.helpers.rest_client.paginators import BasePaginator
from dlt.sources.helpers.requests import Response, Request

class QueryParamPaginator(BasePaginator):
    def __init__(self, page_param: str = "page", initial_page: int = 1):
        super().__init__()
        self.page_param = page_param
        self.page = initial_page

    def init_request(self, request: Request) -> None:
        # This will set the initial page number (e.g., page=1)
        self.update_request(request)

    def update_state(self, response: Response, data: Optional[List[Any]] = None) -> None:
        # Assuming the API returns an empty list when no more data is available
        if not response.json():
            self._has_next_page = False
        else:
            self.page += 1

    def update_request(self, request: Request) -> None:
        if request.params is None:
            request.params = {}
        request.params[self.page_param] = self.page
```

After defining your custom paginator, you can use it with the `RESTClient` by passing an instance of your paginator to the paginator parameter during the client's initialization. Here's how to use the `QueryParamPaginator`:

```py
from dlt.sources.helpers.rest_client import RESTClient

client = RESTClient(
    base_url="https://api.example.com",
    paginator=QueryParamPaginator(page_param="page", initial_page=1)
)

@dlt.resource
def get_data():
    for page in client.paginate("/data"):
        yield page
```

:::tip
[`PageNumberPaginator`](#pagenumberpaginator) that ships with dlt does the same thing, but with more flexibility and error handling. This example is meant to demonstrate how to implement a custom paginator. For most use cases, you should use the [built-in paginators](#paginators).
:::

#### Example 2: Creating a paginator for POST requests

Some APIs use POST requests for pagination, where the next page is fetched by sending a POST request with a cursor or other parameters in the request body. This is frequently used in "search" API endpoints or other endpoints with large payloads. Here's how you could implement a paginator for a case like this:

```py
from typing import Any, List, Optional
from dlt.sources.helpers.rest_client.paginators import BasePaginator
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.requests import Response, Request

class PostBodyPaginator(BasePaginator):
    def __init__(self):
        super().__init__()
        self.cursor = None

    def update_state(self, response: Response, data: Optional[List[Any]] = None) -> None:
        # Assuming the API returns an empty list when no more data is available
        if not response.json():
            self._has_next_page = False
        else:
            self.cursor = response.json().get("next_page_cursor")

    def update_request(self, request: Request) -> None:
        if request.json is None:
            request.json = {}

        # Add the cursor to the request body
        request.json["cursor"] = self.cursor

client = RESTClient(
    base_url="https://api.example.com",
    paginator=PostBodyPaginator()
)

@dlt.resource
def get_data():
    for page in client.paginate("/data"):
        yield page
```

## Authentication

The RESTClient supports various authentication strategies, such as bearer tokens, API keys, and HTTP basic auth, configured through the `auth` parameter of both the `RESTClient` and the `paginate()` method.

The available authentication methods are defined in the `dlt.sources.helpers.rest_client.auth` module:

- [BearerTokenAuth](#bearer-token-authentication)
- [APIKeyAuth](#api-key-authentication)
- [HttpBasicAuth](#http-basic-authentication)
- [OAuth2ClientCredentials](#oauth-20-authorization)

For specific use cases, you can [implement custom authentication](#implementing-custom-authentication) by subclassing the `AuthConfigBase` class from the `dlt.sources.helpers.rest_client.auth` module.
For specific flavors of OAuth 2.0, you can [implement custom OAuth 2.0](#oauth-20-authorization) by subclassing `OAuth2ClientCredentials`.

### Bearer token authentication

Bearer Token Authentication (`BearerTokenAuth`) is an auth method where the client sends a token in the request's Authorization header (e.g., `Authorization: Bearer <token>`). The server validates this token and grants access if the token is valid.

**Parameters:**

- `token`: The bearer token to use for authentication.

**Example:**

```py
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth

client = RESTClient(
    base_url="https://api.example.com",
    auth=BearerTokenAuth(token="your_access_token_here")  # type: ignore
)

for page in client.paginate("/protected/resource"):
    print(page)
```

### API key authentication

API Key Authentication (`ApiKeyAuth`) is an auth method where the client sends an API key in a custom header (e.g., `X-API-Key: <key>`, or as a query parameter).

**Parameters:**

- `name`: The name of the header or query parameter to use for the API key.
- `api_key`: The API key to use for authentication.
- `location`: The location of the API key (`header` or `query`). Defaults to "header".

**Example:**

```py
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import APIKeyAuth

auth = APIKeyAuth(name="X-API-Key", api_key="your_api_key_here", location="header")  # type: ignore

# Create a RESTClient instance with API Key Authentication
client = RESTClient(base_url="https://api.example.com", auth=auth)

response = client.get("/protected/resource")
```

### HTTP basic authentication

HTTP Basic Authentication is a simple authentication scheme built into the HTTP protocol. It sends a username and password encoded in the Authorization header.

**Parameters:**

- `username`: The username for basic authentication.
- `password`: The password for basic authentication.

**Example:**

```py
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import HttpBasicAuth

auth = HttpBasicAuth(username="your_username", password="your_password")  # type: ignore
client = RESTClient(base_url="https://api.example.com", auth=auth)

response = client.get("/protected/resource")
```

### OAuth 2.0 authorization

OAuth 2.0 is a common protocol for authorization. We have implemented two-legged authorization employed for server-to-server authorization because the end user (resource owner) does not need to grant approval.
The REST client acts as the OAuth client, which obtains a temporary access token from the authorization server. This access token is then sent to the resource server to access protected content. If the access token is expired, the OAuth client automatically refreshes it.

Unfortunately, most OAuth 2.0 implementations vary, and thus you might need to subclass `OAuth2ClientCredentials` and implement `build_access_token_request()` to suit the requirements of the specific authorization server you want to interact with.

**Parameters:**
- `access_token_url`: The URL to obtain the temporary access token.
- `client_id`: Client identifier to obtain authorization. Usually issued via a developer portal.
- `client_secret`: Client credential to obtain authorization. Usually issued via a developer portal.
- `access_token_request_data`: A dictionary with data required by the authorization server apart from the `client_id`, `client_secret`, and `"grant_type": "client_credentials"`. Defaults to `None`.
- `default_token_expiration`: The time in seconds after which the temporary access token expires. Defaults to 3600.
- `session`: Custom `requests` session where you can configure default timeouts and retry strategies

**Example:**

```py
from base64 import b64encode
from dlt.common.configuration import configspec
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import OAuth2ClientCredentials

@configspec
class OAuth2ClientCredentialsHTTPBasic(OAuth2ClientCredentials):
    """Used e.g. by Zoom Video Communications, Inc."""
    def build_access_token_request(self) -> Dict[str, Any]:
        authentication: str = b64encode(
            f"{self.client_id}:{self.client_secret}".encode()
        ).decode()
        return {
            "headers": {
                "Authorization": f"Basic {authentication}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            "data": self.access_token_request_data,
        }

oauth = OAuth2ClientCredentialsHTTPBasic(
    access_token_url=dlt.secrets["sources.zoom.access_token_url"],  # "https://zoom.us/oauth/token"
    client_id=dlt.secrets["sources.zoom.client_id"],
    client_secret=dlt.secrets["sources.zoom.client_secret"],
    access_token_request_data={
        "grant_type": "account_credentials",
        "account_id": dlt.secrets["sources.zoom.account_id"],
    },
)
client = RESTClient(base_url="https://api.zoom.us/v2", auth=oauth)

response = client.get("/users")
```



### Implementing custom authentication

You can implement custom authentication by subclassing the `AuthConfigBase` class and implementing the `__call__` method:

```py
from dlt.common.configuration import configspec
from dlt.sources.helpers.rest_client.auth import AuthConfigBase

@configspec
class CustomAuth(AuthConfigBase):
    def __init__(self, token):
        self.token = token

    def __call__(self, request):
        # Modify the request object to include the necessary authentication headers
        request.headers["Authorization"] = f"Custom {self.token}"
        return request
```

Then, you can use your custom authentication class with the `RESTClient`:

```py
client = RESTClient(
    base_url="https://api.example.com",
    auth=CustomAuth(token="your_custom_token_here")
)
```

## Advanced usage

`RESTClient.paginate()` allows you to specify a [custom hook function](https://requests.readthedocs.io/en/latest/user/advanced/#event-hooks) that can be used to modify the response objects. For example, to handle specific HTTP status codes gracefully:

```py
def custom_response_handler(response, *args):
    if response.status_code == 404:
        # Handle not found
        pass

client.paginate("/posts", hooks={"response": [custom_response_handler]})
```

The handler function may raise `IgnoreResponseException` to exit the pagination loop early. This is useful for endpoints that return a 404 status code when there are no items to paginate.

## Shortcut for paginating API responses

The `paginate()` function provides a shorthand for paginating API responses. It takes the same parameters as the `RESTClient.paginate()` method but automatically creates a RESTClient instance with the specified base URL:

```py
from dlt.sources.helpers.rest_client import paginate

for page in paginate("https://api.example.com/posts"):
    print(page)
```

## Retry

You can customize how the RESTClient retries failed requests by editing your `config.toml`.
See more examples and explanations in our [documentation on retry rules](requests#retry-rules).

Example:

```toml
[runtime]
request_max_attempts = 10  # Stop after 10 retry attempts instead of 5
request_backoff_factor = 1.5  # Multiplier applied to the exponential delays. Default is 1
request_timeout = 120  # Timeout in seconds
request_max_retry_delay = 30  # Cap exponential delay to 30 seconds
```

:::note
`RESTClient` retries by default:

```toml
[runtime]
request_timeout=60
request_max_attempts = 5
request_backoff_factor = 1
request_max_retry_delay = 300
```
:::

### Use custom session
You can pass custom `requests` `Session` to `RESTClient`. `dlt` provides its own implementation where you can easily configure
retry strategies, timeouts and other factors. For example:
```py
from dlt.sources.helpers import requests
client = RESTClient(
    base_url="https://api.example.com",
    session=requests.Client(request_timeout=(1.0, 1.0), request_max_attempts=0).session
)
```
will set-up the client for a short connect and read timeouts with no retries.

### URL sanitization and secret protection

The RESTClient automatically sanitizes URLs in logs and error messages to prevent exposure of sensitive information. Query parameters with the following names are automatically redacted:

- `api_key`, `token`, `key`, `access_token`, `apikey`, `api-key`, `access-token`
- `secret`, `password`, `pwd`, `client_secret`

For example, a URL like `https://api.example.com/data?api_key=secret123&page=1` will appear in logs as `https://api.example.com/data?api_key=***&page=1`.


## Troubleshooting

### Debugging HTTP error responses

When debugging API issues, you may need to see the full error response body. By default, error response bodies are not included in exceptions to keep error messages concise.

To enable error response bodies:

```toml
[runtime]
http_show_error_body = true
http_max_error_body_length = 8192  # Maximum characters (default: 8192)
```

Example error with response body enabled:
```text
HTTPError: 400 Client Error: Bad Request for url: https://api.example.com/data?api_key=***
Response body: {"error": "Invalid date format", "code": "INVALID_DATE", "field": "start_date"}
```

### `RESTClient.get()` and `RESTClient.post()` methods

These methods work similarly to the [get()](https://docs.python-requests.org/en/latest/api/#requests.get) and [post()](https://docs.python-requests.org/en/latest/api/#requests.post) functions from the Requests library. They return a [Response](https://docs.python-requests.org/en/latest/api/#requests.Response) object that contains the response data.
You can inspect the `Response` object to get the `response.status_code`, `response.headers`, and `response.content`. For example:

```py
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth

client = RESTClient(base_url="https://api.example.com")
response = client.get("/posts", auth=BearerTokenAuth(token="your_access_token"))  # type: ignore

print(response.status_code)
print(response.headers)
print(response.content)
```

### `RESTClient.paginate()`

Debugging `paginate()` is trickier because it's a generator function that yields [`PageData`](#pagedata) objects. Here are several ways to debug the `paginate()` method:

1. Enable logging to see detailed information about the HTTP requests:

```sh
RUNTIME__LOG_LEVEL=INFO python my_script.py
```

2. Use the [`PageData`](#pagedata) instance to inspect the [request](https://docs.python-requests.org/en/latest/api/#requests.Request)
and [response](https://docs.python-requests.org/en/latest/api/#requests.Response) objects:

```py
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator

client = RESTClient(
    base_url="https://api.example.com",
    paginator=JSONLinkPaginator(next_url_path="pagination.next")
)

for page in client.paginate("/posts"):
    print(page.request)
    print(page.response)
```

3. Use the `hooks` parameter to add custom response handlers to the `paginate()` method:

```py
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth

def response_hook(response, *args):
    print(response.status_code)
    print(f"Content: {response.content}")
    print(f"Request: {response.request.body}")
    # Or import pdb; pdb.set_trace() to debug

for page in client.paginate(
    "/posts",
    auth=BearerTokenAuth(token="your_access_token"),  # type: ignore
    hooks={"response": [response_hook]}
):
    print(page)
```

## Requests wrapper

`dlt` provides a customized [Python Requests](https://requests.readthedocs.io/en/latest/) client with automatic retries and configurable timeouts.

We recommend using this to make API calls in your sources as it makes your pipeline more resilient to intermittent network errors and other random glitches which otherwise can cause the whole pipeline to fail.

The dlt requests client will additionally set the default user-agent header to `dlt/{DLT_VERSION_NAME}`.

For most use cases, this is a drop-in replacement for `requests`, so in places where you would normally do:

```py
import requests
```

You can instead do:

```py
from dlt.sources.helpers import requests
```

And use it just like you would use `requests`:

```py
response = requests.get(
    'https://example.com/api/contacts',
    headers={'Authorization': API_KEY}
)
data = response.json()
...
```

### Retry rules

By default, failing requests are retried up to 5 times with an exponentially increasing delay. That means the first retry will wait 1 second, and the fifth retry will wait 16 seconds.

If all retry attempts fail, the corresponding requests exception is raised. E.g., `requests.HTTPError` or `requests.ConnectionTimeout`.

All standard HTTP server errors trigger a retry. This includes:

* Error status codes:

    All status codes in the `500` range and `429` (too many requests).
    Commonly, servers include a `Retry-After` header with `429` and `503` responses.
    When detected, this value supersedes the standard retry delay.

* Connection and timeout errors

    When the remote server is unreachable, the connection is unexpectedly dropped, or when the request takes longer than the configured `timeout`.

### Customizing retry settings

Many requests settings can be added to the runtime section in your `config.toml`. For example:

```toml
[runtime]
request_max_attempts = 10  # Stop after 10 retry attempts instead of 5
request_backoff_factor = 1.5  # Multiplier applied to the exponential delays. Default is 1
request_timeout = 120  # Timeout in seconds
request_max_retry_delay = 30  # Cap exponential delay to 30 seconds
```

:::note
Default session retries as follows:

```toml
[runtime]
request_timeout=60
request_max_attempts = 5
request_backoff_factor = 1
request_max_retry_delay = 300
```
:::

For more control, you can create your own instance of `dlt.sources.requests.Client` and use that instead of the global client.

This lets you customize which status codes and exceptions to retry on:

```py
from dlt.sources.helpers import requests

http_client = requests.Client(
    status_codes=(403, 500, 502, 503),
    exceptions=(requests.ConnectionError, requests.ChunkedEncodingError)
)
```

and you may even supply a custom retry condition in the form of a predicate.
This is sometimes needed when loading from non-standard APIs which don't use HTTP error codes.

For example:

```py
from dlt.sources.helpers import requests

def retry_if_error_key(response: Optional[requests.Response], exception: Optional[BaseException]) -> bool:
    """Decide whether to retry the request based on whether
    the json response contains an `error` key
    """
    if response is None:
        # Fall back on the default exception predicate.
        return False
    data = response.json()
    return 'error' in data

http_client = Client(
    retry_condition=retry_if_error_key
)
```

:::tip
`requests.Client` is thread safe. We recommend to share sessions across threads for better performance.
:::


### Handling API Rate Limits

HTTP 429 errors indicate you've hit API rate limits. The dlt requests client retries these automatically and respects `Retry-After` headers. If rate limits persist, consider additional mitigation strategies.

- **Check authentication**: Properly authenticated requests often have higher rate limits
- **Review API documentation**: Look for rate limit guidelines and `Retry-After` header usage
- **Add delays**: Use `time.sleep()` or a rate-limiting library to space out requests
- **Implement backoff**: Increase wait times after failures (exponential backoff)
- **Reduce calls**: Batch requests or cache results when possible

> 💡 The dlt requests client already handles basic `429` retries with exponential backoff and respects `Retry-After` headers.

## Advanced configuration

`rest_api_source()` function creates the [dlt source](../../../general-usage/source.md) and lets you configure the following parameters:

- `config`: The REST API configuration dictionary.
- `name`: An optional name for the source.
- `section`: An optional section name in the configuration file.
- `max_table_nesting`: Sets the maximum depth of nested tables above which the remaining nodes are loaded as structs or JSON.
- `root_key` (bool): Enables merging on all resources by propagating the root foreign key to nested tables. This option is most useful if you plan to change the write disposition of a resource to disable/enable merge. Defaults to False.
- `schema`: An explicit `dlt.Schema` instance to be associated with the source. If not present, `dlt` creates a new `Schema` object with the provided `name`. If such `dlt.Schema` already exists in the same folder as the module containing the decorated function, such schema will be loaded from file.
- `schema_contract`: Schema contract settings that will be applied to this resource.
- `parallelized`: If `True`, resource generators will be extracted in parallel with other resources.

### Per-resource parallelization for dependent resources

You can also set `parallelized` on individual dependent resources (transformers) to fetch child data for multiple parent items concurrently. When enabled, each parent item's child fetch runs as a deferred callable in dlt's thread pool rather than sequentially in a loop.

```py
config: RESTAPIConfig = {
    "client": {"base_url": "https://api.example.com"},
    "resources": [
        "posts",
        {
            "name": "post_comments",
            "parallelized": True,
            "endpoint": {
                "path": "posts/{resources.posts.id}/comments",
            },
        },
    ],
}
```

:::note
When `parallelized` is set on a dependent resource, all pages of child data for a single parent item are collected into memory before being yielded. For parent items with a large number of child pages, this increases memory usage compared to the default streaming behavior.
:::

### Headers configuration

The REST API source supports configuring custom headers at both the client level and the endpoint level. This allows you to send additional HTTP headers with your API requests, which is useful for some use cases.

Headers can be configured in two places:

1. [Client-level headers](#client-level-headers): Static headers applied to all requests for all resources.
2. [Endpoint-level headers](#endpoint-level-headers): Headers applied only to requests for a specific resource (support placeholder interpolation). These are also can be specified via `resource_defaults`.

When both client-level and endpoint-level headers are specified, endpoint-level headers override client-level headers for the same header names.

##### Client-level headers

Client-level headers are static and applied to all requests:

```py
config: RESTAPIConfig = {
    "client": {
        "base_url": "https://api.example.com",
        "headers": {
            "User-Agent": "MyApp/1.0",
            "Accept": "application/json",
            "X-API-Version": "v2"
        }
    },
    "resources": ["posts", "comments"]
}
```

##### Endpoint-level headers

```py
config: RESTAPIConfig = {
    "client": {
        "base_url": "https://api.example.com",
        "headers": {
            "User-Agent": "MyApp/1.0"
        }
    },
    "resources": [
        {
            "name": "posts",
            "endpoint": {
                "path": "posts",
                "headers": {
                    "Content-Type": "application/vnd.api+json",
                    "X-Custom-Header": "posts-specific-value"
                }
            }
        }
    ]
}
```

#### Dynamic headers with placeholders

[Endpoint-level headers](#endpoint-level-headers) support dynamic values using placeholder interpolation. This allows you to reference data from parent resources and incremental values.

:::note
Client-level headers do not support placeholder interpolation. If you need dynamic headers with placeholders, you must define them at the endpoint level.
:::

##### Example: headers with resource data

You can reference fields from parent resources in header values:

```py
config: RESTAPIConfig = {
    "client": {
        "base_url": "https://api.example.com"
    },
    "resources": [
        "posts",
        {
            "name": "comments",
            "endpoint": {
                "path": "posts/{resources.posts.id}/comments",
                "headers": {
                    "X-Post-ID": "{resources.posts.id}",
                    "X-Post-Title": "{resources.posts.title}"
                }
            }
        }
    ]
}
```

In this example, for each post, the `comments` resource will include headers with the post's ID and title.

##### Example: headers with incremental values

You can also use incremental values in headers:

```py
config: RESTAPIConfig = {
    "client": {
        "base_url": "https://api.example.com"
    },
    "resources": [
        {
            "name": "events",
            "endpoint": {
                "path": "events",
                "headers": {
                    "X-Since-Timestamp": "{incremental.start_value}"
                },
                "incremental": {
                    "cursor_path": "updated_at",
                    "initial_value": "2024-01-01T00:00:00Z"
                }
            }
        }
    ]
}
```

See the [incremental loading](./basic.md#incremental-loading) section for more details.

### Response actions

The `response_actions` field in the endpoint configuration allows you to specify how to handle specific responses or all responses from the API. For example, responses with specific status codes or content substrings can be ignored.
Additionally, all responses or only responses with specific status codes or content substrings can be transformed with a custom callable, such as a function. This callable is passed on to the requests library as a [response hook](https://requests.readthedocs.io/en/latest/user/advanced/#event-hooks). The callable can modify the response object and must return it for the modifications to take effect.

:::warning Experimental Feature
This is an experimental feature and may change in future releases.
:::

**Fields:**

- `status_code` (int, optional): The HTTP status code to match.
- `content` (str, optional): A substring to search for in the response content.
- `action` (str or Callable or List[Callable], optional): The action to take when the condition is met. Currently supported actions:
  - `"ignore"`: Ignore the response.
  - a callable accepting and returning the response object.
  - a list of callables, each accepting and returning the response object.


#### Example A

```py
{
    "path": "issues",
    "response_actions": [
        {"status_code": 404, "action": "ignore"},
        {"content": "Not found", "action": "ignore"},
        {"status_code": 200, "content": "some text", "action": "ignore"},
    ],
}
```

In this example, the source will ignore responses with a status code of 404, responses with the content "Not found", and responses with a status code of 200 _and_ content "some text".

#### Example B

```py
from requests.models import Response
from dlt.common import json

def set_encoding(response, *args, **kwargs):
    # Sets the encoding in case it's not correctly detected
    response.encoding = 'windows-1252'
    return response


def add_and_remove_fields(response: Response, *args, **kwargs) -> Response:
    payload = response.json()
    for record in payload["data"]:
        record["custom_field"] = "foobar"
        record.pop("email", None)
    modified_content: bytes = json.dumps(payload).encode("utf-8")
    response._content = modified_content
    return response


source_config = {
    "client": {
        # ...
    },
    "resources": [
        {
            "name": "issues",
            "endpoint": {
                "path": "issues",
                "response_actions": [
                    set_encoding,
                    {
                        "status_code": 200,
                        "content": "some text",
                        "action": add_and_remove_fields,
                    },
                ],
            },
        },
    ],
}
```

In this example, the resource will set the correct encoding for all responses first. Thereafter, for all responses that have the status code 200, we will add a field `custom_field` and remove the field `email`.

#### Example C

```py
def set_encoding(response, *args, **kwargs):
    # Sets the encoding in case it's not correctly detected
    response.encoding = 'windows-1252'
    return response

source_config = {
    "client": {
        # ...
    },
    "resources": [
        {
            "name": "issues",
            "endpoint": {
                "path": "issues",
                "response_actions": [
                    set_encoding,
                ],
            },
        },
    ],
}
```

In this example, the resource will set the correct encoding for all responses. More callables can be added to the list of response_actions.


### Setup timeouts and retry strategies
`rest_api` uses `dlt` custom sessions and `RESTClient` to access http(s) endpoints. You can use them to configure timeout, retries and other aspects. For example:
```py
from dlt.sources.helpers import requests

source_config: RESTAPIConfig = {
    "client": {
        "session": requests.Client(request_timeout=(1.0, 1.0), request_max_attempts=0)
    },
}
```
will set-up all endpoints to use a short connect and read timeouts with no retries.
Most settings can be configured using `toml` files or environment variables.

:::note
By default, we set connection timeout and read timeout to 60 seconds, with
5 retry attempts without backoff.
:::
