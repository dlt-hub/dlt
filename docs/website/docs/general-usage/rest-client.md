# Reading data from RESTful APIs

RESTful APIs are a common way to interact with web services. They are based on the HTTP protocol and are used to read and write data from and to a web server. dlt provides a simple way to read data from RESTful APIs using two helper methods: [a wrapper around the  Requests library](../reference/performance#using-the-built-in-requests-client) and a `RESTClient` class.

:::tip
There's also shorthand function to read from paginated APIs. Check out the [paginate()](#shortcut-for-paginating-api-responses) function.
:::


The `RESTClient` class offers a powerful interface for interacting with RESTful APIs, including features like:
- automatic pagination,
- various authentication mechanisms,
- customizable request/response handling.

This guide shows how to use the `RESTClient` class to read data APIs focusing on its `paginate()` method to fetch data from paginated API responses.

## Quick example

Here's a simple pipeline that reads issues from the [dlt GitHub repository](https://github.com/dlt-hub/dlt/issues). The API endpoint is https://api.github.com/repos/dlt-hub/dlt/issues. The result is "paginated", meaning that the API returns a limited number of issues per page. The `paginate()` method iterates over all pages and yields the results which are then processed by the pipeline.

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
2. Issues endpoint returns a list of issues. Since there could be hundreds of issues, the API "paginates" the results: it returns a limited number of issues in each response along with a link to the next batch of issues (or "page"). The `paginate()` method iterates over all pages and yields the batches of issues.
3. Here we specify the address of the endpoint we want to read from: `/repos/dlt-hub/dlt/issues`.
4. We pass the parameters to the actual API call to control the data we get back. In this case, we ask for 100 issues per page (`"per_page": 100`), sorted by the last update date (`"sort": "updated"`) in descending order (`"direction": "desc"`).
5. We yield the page from the resource function to the pipeline. The `page` is an instance of the [`PageData`](#pagedata) and contains the data from the current page of the API response and some metadata.

Note that we do not explicitly specify the pagination parameters in the example. The `paginate()` method handles pagination automatically: it detects the pagination mechanism used by the API from the response and paginates accordingly. What if you need to specify the pagination method and parameters explicitly? Let's see how to do that in a different example below.

### Explicitly specifying pagination parameters

```py
import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import JSONResponsePaginator

github_client = RESTClient(
    base_url="https://pokeapi.co/api/v2",
    paginator=JSONResponsePaginator(next_url_path="next")    # (1)
    data_selector="results",                                 # (2)
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
1. We create a `RESTClient` instance with the base URL of the API: in this case, the [PokéAPI](https://pokeapi.co/). We also specify the paginator to use explicitly: `JSONResponsePaginator` with the `next_url_path` set to `"next"`. This tells the paginator to look for the next page URL in the `next` key of the JSON response.
2. In `data_selector` we specify the JSON path to extract the data from the response. This is used to extract the data from the response JSON.
3. By default the number of items per page is limited to 20. We override this by specifying the `limit` parameter in the API call.


## Understanding the `RESTClient` Class

The `RESTClient` class is initialized with parameters that define its behavior for making API requests:

- `base_url`: The root URL of the API. All requests will be made relative to this URL.
- `headers`: Default headers to include in every request. This can be used to set common headers like `User-Agent` or other custom headers.
- `auth`: The authentication configuration. See the [Authentication](#authentication) section for more details.
- `paginator`: A paginator instance for handling paginated responses. See the [Paginators](#paginators) below.
- `data_selector`: A [JSONPath selector](https://github.com/h2non/jsonpath-ng?tab=readme-ov-file#jsonpath-syntax) for extracting data from the responses. This defines a way to extract the data from the response JSON. Only used when paginating.
- `session`: An HTTP session for making requests. This is a custom session object that can be used to set up custom behavior for requests.

## Making basic requests

To perform basic GET and POST requests, use the get() and post() methods respectively. This works similarly to the requests library:

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

If `paginator` is not specified, the `paginate()` method will attempt to automatically detect the pagination mechanism used by the API. If the API uses a standard pagination mechanism like having a `next` link in the response's headers or JSON body, the `paginate()` method will handle this automatically. Otherwise, you can specify a paginator object explicitly or implement a custom paginator.

### PageData

Each `PageData` instance contains the data for a single page, along with context like the original request and response objects, allowing for detailed inspection. The `PageData` is a list-like object that contains the following attributes:

- `request`: The original request object.
- `response`: The response object.
- `paginator`: The paginator object used to paginate the response.
- `auth`: The authentication object used for the request.

### Paginators

Paginators are used to handle paginated responses. The `RESTClient` class comes with built-in paginators for common pagination mechanisms.

#### JSONResponsePaginator

`JSONResponsePaginator` is designed for APIs where the next page URL is included in the response's JSON body. This paginator uses a JSON path to locate the next page URL within the JSON response.

##### Parameters:

- `next_url_path`: A JSON path string pointing to the key in the JSON response that contains the next page URL.

###### Example:

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

##### Parameters:

- `links_next_key`: The relation type (rel) to identify the next page link within the Link header. Defaults to "next".

Note: normally, you don't need to specify this paginator explicitly, as it is used automatically when the API returns a `Link` header. On rare occasions, you may need to specify when the API uses a different relation type.

#### OffsetPaginator

`OffsetPaginator` handles pagination based on an offset and limit in the query parameters. This works only if the API returns the total number of items in the response.

##### Parameters:

- `initial_limit`: The maximum number of items to retrieve in each request.
- `initial_offset`: The starting point for the first request. Defaults to `0`.
- `offset_param`: The name of the query parameter used to specify the offset. Defaults to `"offset"`.
- `limit_param`: The name of the query parameter used to specify the limit. Defaults to `"limit"`.
- `total_path`: A JSONPath expression pointing to the total number of items in the dataset, used to determine if more pages are available. Defaults to `"total"`.

###### Example:

Assuming an API endpoint `https://api.example.com/items` supports pagination with `offset` and `limit` parameters.
E.g. `https://api.example.com/items?offset=0&limit=100`, `https://api.example.com/items?offset=100&limit=100`, etc. And includes the total count in its responses, e.g.:

```json
{
  "items": [...],
  "total": 1000
}
```

You can paginate through responses from this API using `OffsetPaginator`:

```py
client = RESTClient(
    base_url="https://api.example.com",
    paginator=OffsetPaginator(
        initial_limit=100,
        total_path="total"
    )
)
```

#### JSONResponseCursorPaginator

`JSONResponseCursorPaginator` handles pagination based on a cursor in the JSON response.

##### Parameters:

- `cursor_path`: A JSONPath expression pointing to the cursor in the JSON response. This cursor is used to fetch subsequent pages. Defaults to `"cursors.next"`.
- `cursor_param`: The query parameter used to send the cursor value in the next request. Defaults to `"after"`.

###### Example:

Consider an API endpoint `https://api.example.com/data` returning a structure where a cursor to the next page is included in the response:

```json
{
  "items": [...],
  "cursors": {
    "next": "cursor_string_for_next_page"
  }
}

To paginate through responses from this API, use `JSONResponseCursorPaginator` with `cursor_path` set to "cursors.next":

```py
client = RESTClient(
    base_url="https://api.example.com",
    paginator=JSONResponseCursorPaginator(cursor_path="cursors.next")
)
```

#### Implementing Custom Paginators

When working with APIs that use non-standard pagination schemes, or when you need more control over the pagination process, you can implement a custom paginator by subclassing the `BasePaginator` class and `update_state` and `update_request` methods:

- `update_state(response: Response) -> None`: This method updates the paginator's state based on the response of the API call. Typically, you extract pagination details (like the next page reference) from the response and store them in the paginator instance.

- `update_request(request: Request) -> None`: Before making the next API call in `RESTClient.paginate` method, `update_request` is used to modify the request with the necessary parameters to fetch the next page (based on the current state of the paginator). For example, you can add query parameters to the request, or modify the URL.

##### Example: Creating a Query Parameter Paginator

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
    paginator=QueryParamPaginator(page_param="page", initial_page=1) # Or simply QueryParamPaginator()
)

@dlt.resource
def get_data():
    for page in client.paginate("/data"):
        yield page
```

## Authentication

The RESTClient supports various authentication strategies, such as bearer tokens, API keys, and HTTP basic auth, configured through the `auth` parameter of both the `RESTClient` and the `paginate()` method.

The available authentication methods are defined in the `dlt.sources.helpers.rest_client.auth` module.

### Bearer Token Authentication

Bearer Token Authentication (`BearerTokenAuth`) is an auth method where the client sends a token in the request's Authorization header (e.g. `Authorization: Bearer <token>`). The server validates this token and grants access if the token is valid.

#### Parameters:

- `token`: The bearer token to use for authentication.

#### Example:

```py
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth

client = RESTClient(
    base_url="https://api.example.com",
    auth=BearerTokenAuth(token="your_access_token_here")
)

for page in client.paginate("/protected/resource"):
    print(page)
```

### API Key Authentication

API Key Authentication (`ApiKeyAuth`) is an auth method where the client sends an API key in a custom header (e.g. `X-API-Key: <key>`, or as a query parameter).

#### Parameters:

- `name`: The name of the header or query parameter to use for the API key.
- `api_key`: The API key to use for authentication.
- `location`: The location of the API key (`header` or `query`). Defaults to "header".

#### Example:

```py
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import APIKeyAuth

auth = APIKeyAuth(name="X-API-Key", api_key="your_api_key_here", location="header")

# Create a RESTClient instance with API Key Authentication
client = RESTClient(base_url="https://api.example.com", auth=auth)

response = client.get("/protected/resource")
```

### HTTP Basic Authentication

HTTP Basic Authentication is a simple authentication scheme built into the HTTP protocol. It sends a username and password encoded in the Authorization header.

#### Parameters:

- `username`: The username for basic authentication.
- `password`: The password for basic authentication.

#### Example:

```py
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import HttpBasicAuth

auth = HttpBasicAuth(username="your_username", password="your_password")
client = RESTClient(base_url="https://api.example.com", auth=auth)

response = client.get("/protected/resource")
```

## Advanced Usage

`RESTClient.paginate()` allows to specify a custom hook function that can be used to modify the response objects. For example, to handle specific HTTP status codes gracefully:

```py
def custom_response_handler(response):
    if response.status_code == 404:
        # Handle not found
        pass

client.paginate("/posts", hooks={"response": [custom_response_handler]})
```

The handler function may raise `IgnoreResponseException` to exit the pagination loop early. This is useful for the enpoints that return a 404 status code when there are no items to paginate.

## Shortcut for Paginating API Responses

The `paginate()` helper function provides a shorthand for paginating API responses. It takes the same parameters as the `RESTClient.paginate()` method but automatically creates a RESTClient instance with the specified base URL:

```py
from dlt.sources.helpers.rest_client import paginate

for page in paginate("https://api.example.com/posts"):
    print(page)
```