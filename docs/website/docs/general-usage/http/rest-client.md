---
title: RESTClient
description: Learn how to use the RESTClient class to interact with RESTful APIs
keywords: [api, http, rest, request, extract, restclient, client, pagination, json, response, data_selector, session, auth, paginator, jsonresponsepaginator, headerlinkpaginator, offsetpaginator, jsonresponsecursorpaginator, queryparampaginator, bearer, token, authentication]
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
from dlt.sources.helpers.rest_client.paginators import JSONResponsePaginator

client = RESTClient(
    base_url="https://api.example.com",
    headers={"User-Agent": "MyApp/1.0"},
    auth=BearerTokenAuth(token="your_access_token_here"),  # type: ignore
    paginator=JSONResponsePaginator(next_url_path="pagination.next"),
    data_selector="data",
    session=MyCustomSession()
)
```

The `RESTClient` class is initialized with the following parameters:

- `base_url`: The root URL of the API. All requests will be made relative to this URL.
- `headers`: Default headers to include in every request. This can be used to set common headers like `User-Agent` or other custom headers.
- `auth`: The authentication configuration. See the [Authentication](#authentication) section for more details.
- `paginator`: A paginator instance for handling paginated responses. See the [Paginators](#paginators) below.
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
If `paginator` is not specified, the `paginate()` method will attempt to automatically detect the pagination mechanism used by the API. If the API uses a standard pagination mechanism like having a `next` link in the response's headers or JSON body, the `paginate()` method will handle this automatically. Otherwise, you can specify a paginator object explicitly or implement a custom paginator.
:::

### Selecting data from the response

When paginating through API responses, the `RESTClient` tries to automatically extract the data from the response. Sometimes though you may need to explicitly
specify how to extract the data from the response JSON.

Use `data_selector` parameter of the `RESTClient` class or the `paginate()` method to tell the client how to extract the data.
`data_selector` is a [JSONPath](https://github.com/h2non/jsonpath-ng?tab=readme-ov-file#jsonpath-syntax) expression that points to the key in
the JSON that contains the data to be extracted.

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

Each `PageData` instance contains the data for a single page, along with context such as the original request and response objects, allowing for detailed inspection.. The `PageData` is a list-like object that contains the following attributes:

- `request`: The original request object.
- `response`: The response object.
- `paginator`: The paginator object used to paginate the response.
- `auth`: The authentication object used for the request.

### Paginators

Paginators are used to handle paginated responses. The `RESTClient` class comes with built-in paginators for common pagination mechanisms:

- [JSONResponsePaginator](#jsonresponsepaginator) - link to the next page is included in the JSON response.
- [HeaderLinkPaginator](#headerlinkpaginator) - link to the next page is included in the response headers.
- [OffsetPaginator](#offsetpaginator) - pagination based on offset and limit query parameters.
- [PageNumberPaginator](#pagenumberpaginator) - pagination based on page numbers.
- [JSONResponseCursorPaginator](#jsonresponsecursorpaginator) - pagination based on a cursor in the JSON response.

If the API uses a non-standard pagination, you can [implement a custom paginator](#implementing-a-custom-paginator) by subclassing the `BasePaginator` class.

#### JSONResponsePaginator

`JSONResponsePaginator` is designed for APIs where the next page URL is included in the response's JSON body. This paginator uses a JSONPath to locate the next page URL within the JSON response.

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

To paginate this response, you can use the `JSONResponsePaginator` with the `next_url_path` set to `"pagination.next"`:

```py
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import JSONResponsePaginator

client = RESTClient(
    base_url="https://api.example.com",
    paginator=JSONResponsePaginator(next_url_path="pagination.next")
)

@dlt.resource
def get_data():
    for page in client.paginate("/posts"):
        yield page
```


#### HeaderLinkPaginator

This paginator handles pagination based on a link to the next page in the response headers (e.g., the `Link` header, as used by GitHub).

**Parameters:**

- `links_next_key`: The relation type (rel) to identify the next page link within the Link header. Defaults to "next".

Note: normally, you don't need to specify this paginator explicitly, as it is used automatically when the API returns a `Link` header. On rare occasions, you may
need to specify the paginator when the API uses a different relation type.

#### OffsetPaginator

`OffsetPaginator` handles pagination based on an offset and limit in the query parameters.

**Parameters:**

- `limit`: The maximum number of items to retrieve in each request.
- `offset`: The initial offset for the first request. Defaults to `0`.
- `offset_param`: The name of the query parameter used to specify the offset. Defaults to `"offset"`.
- `limit_param`: The name of the query parameter used to specify the limit. Defaults to `"limit"`.
- `total_path`: A JSONPath expression for the total number of items. If not provided, pagination is controlled by `maximum_offset`.
- `maximum_offset`: Optional maximum offset value. Limits pagination even without total count.

**Example:**

Assuming an API endpoint `https://api.example.com/items` supports pagination with `offset` and `limit` parameters.
E.g. `https://api.example.com/items?offset=0&limit=100`, `https://api.example.com/items?offset=100&limit=100`, etc. And includes the total count in its responses, e.g.:

```json
{
  "items": ["one", "two", "three"],
  "total": 1000
}
```

You can paginate through responses from this API using `OffsetPaginator`:

```py
client = RESTClient(
    base_url="https://api.example.com",
    paginator=OffsetPaginator(
        limit=100,
        total_path="total"
    )
)
```

In a different scenario where the API does not provide the total count, you can use `maximum_offset` to limit the pagination:

```py
client = RESTClient(
    base_url="https://api.example.com",
    paginator=OffsetPaginator(
        limit=100,
        maximum_offset=1000,
        total_path=None
    )
)
```

Note, that in this case, the `total_path` parameter is set explicitly to `None` to indicate that the API does not provide the total count.

#### PageNumberPaginator

`PageNumberPaginator` works by incrementing the page number for each request.

**Parameters:**

- `initial_page`: The starting page number. Defaults to `1`.
- `page_param`: The query parameter name for the page number. Defaults to `"page"`.
- `total_path`: A JSONPath expression for the total number of pages. If not provided, pagination is controlled by `maximum_page`.
- `maximum_page`: Optional maximum page number. Stops pagination once this page is reached.

**Example:**

Assuming an API endpoint `https://api.example.com/items` paginates by page numbers and provides a total page count in its responses, e.g.:

```json
{
  "items": ["one", "two", "three"],
  "total_pages": 10
}
```

You can paginate through responses from this API using `PageNumberPaginator`:

```py
client = RESTClient(
    base_url="https://api.example.com",
    paginator=PageNumberPaginator(
        total_path="total_pages"  # Uses the total page count from the API
    )
)
```

If the API does not provide the total number of pages:

```py
client = RESTClient(
    base_url="https://api.example.com",
    paginator=PageNumberPaginator(
        maximum_page=5,  # Stops after fetching 5 pages
        total_path=None
    )
)
```

Note, that in the case above, the `total_path` parameter is set explicitly to `None` to indicate that the API does not provide the total count.

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

When working with APIs that use non-standard pagination schemes, or when you need more control over the pagination process, you can implement a custom paginator by subclassing the `BasePaginator` class and `update_state` and `update_request` methods:

- `update_state(response: Response) -> None`: This method updates the paginator's state based on the response of the API call. Typically, you extract pagination details (like the next page reference) from the response and store them in the paginator instance.

- `update_request(request: Request) -> None`: Before making the next API call in `RESTClient.paginate` method, `update_request` is used to modify the request with the necessary parameters to fetch the next page (based on the current state of the paginator). For example, you can add query parameters to the request, or modify the URL.

#### Example 1: creating a query parameter paginator

Suppose an API uses query parameters for pagination, incrementing an page parameter for each subsequent page, without providing direct links to next pages in its responses. E.g. `https://api.example.com/posts?page=1`, `https://api.example.com/posts?page=2`, etc. Here's how you could implement a paginator for this scheme:

```py
from dlt.sources.helpers.rest_client.paginators import BasePaginator
from dlt.sources.helpers.requests import Response, Request

class QueryParamPaginator(BasePaginator):
    def __init__(self, page_param: str = "page", initial_page: int = 1):
        super().__init__()
        self.page_param = page_param
        self.page = initial_page

    def update_state(self, response: Response) -> None:
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

#### Example 2: creating a paginator for POST requests

Some APIs use POST requests for pagination, where the next page is fetched by sending a POST request with a cursor or other parameters in the request body. This is frequently used in "search" API endpoints or other endpoints with big payloads. Here's how you could implement a paginator for a case like this:

```py
from dlt.sources.helpers.rest_client.paginators import BasePaginator
from dlt.sources.helpers.requests import Response, Request

class PostBodyPaginator(BasePaginator):
    def __init__(self):
        super().__init__()
        self.cursor = None

    def update_state(self, response: Response) -> None:
        # Assuming the API returns an empty list when no more data is available
        if not response.json():
            self._has_next_page = False
        else:
            self.cursor = response.json().get("cursor")

    def update_request(self, request: Request) -> None:
        if request.json is None:
            request.json = {}

        # Add the cursor to the request body
        request.json["cursor"] = self.cursor
```

## Authentication

The RESTClient supports various authentication strategies, such as bearer tokens, API keys, and HTTP basic auth, configured through the `auth` parameter of both the `RESTClient` and the `paginate()` method.

The available authentication methods are defined in the `dlt.sources.helpers.rest_client.auth` module:

- [BearerTokenAuth](#bearer-token-authentication)
- [APIKeyAuth](#api-key-authentication)
- [HttpBasicAuth](#http-basic-authentication)

For specific use cases, you can [implement custom authentication](#implementing-custom-authentication) by subclassing the `AuthConfigBase` class.

### Bearer token authentication

Bearer Token Authentication (`BearerTokenAuth`) is an auth method where the client sends a token in the request's Authorization header (e.g. `Authorization: Bearer <token>`). The server validates this token and grants access if the token is valid.

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

API Key Authentication (`ApiKeyAuth`) is an auth method where the client sends an API key in a custom header (e.g. `X-API-Key: <key>`, or as a query parameter).

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

`RESTClient.paginate()` allows to specify a custom hook function that can be used to modify the response objects. For example, to handle specific HTTP status codes gracefully:

```py
def custom_response_handler(response):
    if response.status_code == 404:
        # Handle not found
        pass

client.paginate("/posts", hooks={"response": [custom_response_handler]})
```

The handler function may raise `IgnoreResponseException` to exit the pagination loop early. This is useful for the enpoints that return a 404 status code when there are no items to paginate.

## Shortcut for paginating API responses

The `paginate()` function provides a shorthand for paginating API responses. It takes the same parameters as the `RESTClient.paginate()` method but automatically creates a RESTClient instance with the specified base URL:

```py
from dlt.sources.helpers.rest_client import paginate

for page in paginate("https://api.example.com/posts"):
    print(page)
```