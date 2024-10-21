---
title: RESTClient
description: Learn how to use the RESTClient class to interact with RESTful APIs
keywords: [api, http, rest, request, extract, restclient, client, pagination, json, response, data_selector, session, auth, paginator, JSONLinkPaginator, headerlinkpaginator, offsetpaginator, jsonresponsecursorpaginator, queryparampaginator, bearer, token, authentication]
---

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

## Paginating API responses

The `RESTClient.paginate()` method is specifically designed to handle paginated responses, yielding `PageData` instances for each page:

```py
for page in client.paginate("/posts"):
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

- [JSONLinkPaginator](#JSONLinkPaginator) - link to the next page is included in the JSON response.
- [HeaderLinkPaginator](#headerlinkpaginator) - link to the next page is included in the response headers.
- [OffsetPaginator](#offsetpaginator) - pagination based on offset and limit query parameters.
- [PageNumberPaginator](#pagenumberpaginator) - pagination based on page numbers.
- [JSONResponseCursorPaginator](#jsonresponsecursorpaginator) - pagination based on a cursor in the JSON response.

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
- `limit_param`: The name of the query parameter used to specify the limit. Defaults to `"limit"`.
- `total_path`: A JSONPath expression for the total number of items. If not provided, pagination is controlled by `maximum_offset` and `stop_after_empty_page`.
- `maximum_offset`: Optional maximum offset value. Limits pagination even without a total count.
- `stop_after_empty_page`: Whether pagination should stop when a page contains no result items. Defaults to `True`.

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

You can disable automatic stoppage of pagination by setting `stop_after_empty_page = False`. In this case, you must provide either `total_path` or `maximum_offset` to guarantee that the paginator terminates.

#### PageNumberPaginator

`PageNumberPaginator` works by incrementing the page number for each request.

**Parameters:**

- `base_page`: The index of the initial page from the API perspective. Normally, it's 0-based or 1-based (e.g., 1, 2, 3, ...) indexing for the pages. Defaults to 0.
- `page`: The page number for the first request. If not provided, the initial value will be set to `base_page`.
- `page_param`: The query parameter name for the page number. Defaults to `"page"`.
- `total_path`: A JSONPath expression for the total number of pages. If not provided, pagination is controlled by `maximum_page` and `stop_after_empty_page`.
- `maximum_page`: Optional maximum page number. Stops pagination once this page is reached.
- `stop_after_empty_page`: Whether pagination should stop when a page contains no result items. Defaults to `True`.

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

You can disable automatic stoppage of pagination by setting `stop_after_empty_page = False`. In this case, you must provide either `total_path` or `maximum_page` to guarantee that the paginator terminates.

#### JSONResponseCursorPaginator

`JSONResponseCursorPaginator` handles pagination based on a cursor in the JSON response.

**Parameters:**

- `cursor_path`: A JSONPath expression pointing to the cursor in the JSON response. This cursor is used to fetch subsequent pages. Defaults to `"cursors.next"`.
- `cursor_param`: The query parameter used to send the cursor value in the next request. Defaults to `"after"`.

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

To paginate through responses from this API, use `JSONResponseCursorPaginator` with `cursor_path` set to "cursors.next":

```py
client = RESTClient(
    base_url="https://api.example.com",
    paginator=JSONResponseCursorPaginator(cursor_path="cursors.next")
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

**Example:**

```py
from base64 import b64encode
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import OAuth2ClientCredentials

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

auth = OAuth2ClientCredentialsHTTPBasic(
    access_token_url=dlt.secrets["sources.zoom.access_token_url"],  # "https://zoom.us/oauth/token"
    client_id=dlt.secrets["sources.zoom.client_id"],
    client_secret=dlt.secrets["sources.zoom.client_secret"],
    access_token_request_data={
        "grant_type": "account_credentials",
        "account_id": dlt.secrets["sources.zoom.account_id"],
    },
)
client = RESTClient(base_url="https://api.zoom.us/v2", auth=auth)

response = client.get("/users")
```



### Implementing custom authentication

You can implement custom authentication by subclassing the `AuthConfigBase` class and implementing the `__call__` method:

```py
from dlt.sources.helpers.rest_client.auth import AuthConfigBase

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
def custom_response_handler(response):
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

## Troubleshooting

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

1. Enable [logging](../../running-in-production/running.md#set-the-log-level-and-format) to see detailed information about the HTTP requests:

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

def response_hook(response, **kwargs):
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
