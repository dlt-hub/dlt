---
sidebar_label: paginators
title: sources.helpers.rest_client.paginators
---

## BasePaginator Objects

```python
class BasePaginator(ABC)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/helpers/rest_client/paginators.py#L11)

A base class for all paginator implementations. Paginators are used
to handle paginated responses from RESTful APIs.

See `RESTClient.paginate()` for example usage.

### has\_next\_page

```python
@property
def has_next_page() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/helpers/rest_client/paginators.py#L22)

Determines if there is a next page available.

**Returns**:

- `bool` - True if a next page is available, otherwise False.

### init\_request

```python
def init_request(request: Request) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/helpers/rest_client/paginators.py#L30)

Initializes the request object with parameters for the first
pagination request.

This method can be overridden by subclasses to include specific
initialization logic.

**Arguments**:

- `request` _Request_ - The request object to be initialized.

### update\_state

```python
@abstractmethod
def update_state(response: Response, data: Optional[List[Any]] = None) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/helpers/rest_client/paginators.py#L43)

Updates the paginator's state based on the response from the API.

This method should extract necessary pagination details (like next page
references) from the response and update the paginator's state
accordingly. It should also set the `_has_next_page` attribute to
indicate if there is a next page available.

**Arguments**:

- `response` _Response_ - The response object from the API request.

### update\_request

```python
@abstractmethod
def update_request(request: Request) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/helpers/rest_client/paginators.py#L57)

Updates the request object with arguments for fetching the next page.

This method should modify the request object to include necessary
details (like URLs or parameters) for requesting the next page based on
the current state of the paginator.

**Arguments**:

- `request` _Request_ - The request object to be updated for the next
  page fetch.

## SinglePagePaginator Objects

```python
class SinglePagePaginator(BasePaginator)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/helpers/rest_client/paginators.py#L74)

A paginator for single-page API responses.

## RangePaginator Objects

```python
class RangePaginator(BasePaginator)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/helpers/rest_client/paginators.py#L84)

A base paginator class for paginators that use a numeric parameter
for pagination, such as page number or offset.

See `PageNumberPaginator` and `OffsetPaginator` for examples.

### \_\_init\_\_

```python
def __init__(param_name: str,
             initial_value: int,
             value_step: int,
             base_index: int = 0,
             maximum_value: Optional[int] = None,
             total_path: Optional[jsonpath.TJsonPath] = None,
             error_message_items: str = "items",
             stop_after_empty_page: Optional[bool] = True)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/helpers/rest_client/paginators.py#L91)

**Arguments**:

- `param_name` _str_ - The query parameter name for the numeric value.
  For example, 'page'.
- `initial_value` _int_ - The initial value of the numeric parameter.
- `value_step` _int_ - The step size to increment the numeric parameter.
- `base_index` _int, optional_ - The index of the initial element.
  Used to define 0-based or 1-based indexing. Defaults to 0.
- `maximum_value` _int, optional_ - The maximum value for the numeric parameter.
  If provided, pagination will stop once this value is reached
  or exceeded, even if more data is available. This allows you
  to limit the maximum range for pagination.
  If not provided, `total_path` must be specified. Defaults to None.
- `total_path` _jsonpath.TJsonPath, optional_ - The JSONPath expression
  for the total number of items. For example, if the JSON response is
- ``{"items"` - [...], "total": 100}`, the `total_path` would be 'total'.
  If not provided, `maximum_value` must be specified.
- `error_message_items` _str_ - The name of the items in the error message.
  Defaults to 'items'.
- `stop_after_empty_page` _bool_ - Whether pagination should stop when
  a page contains no result items. Defaults to `True`.

## PageNumberPaginator Objects

```python
class PageNumberPaginator(RangePaginator)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/helpers/rest_client/paginators.py#L194)

A paginator that uses page number-based pagination strategy.

For example, consider an API located at `https://api.example.com/items`
that supports pagination through page number and page size query parameters,
and provides the total number of pages in its responses, as shown below:

    {
        "items": [...],
        "total_pages": 10
    }

To use `PageNumberPaginator` with such an API, you can instantiate `RESTClient`
as follows:

    from dlt.sources.helpers.rest_client import RESTClient

    client = RESTClient(
        base_url="https://api.example.com",
        paginator=PageNumberPaginator(
            total_path="total_pages"
        )
    )

    @dlt.resource
    def get_items():
        for page in client.paginate("/items", params={"size": 100}):
            yield page

Note that we pass the `size` parameter in the initial request to the API.
The `PageNumberPaginator` will automatically increment the page number for
each subsequent request until all items are fetched.

If the API does not provide the total number of pages, you can use the
`maximum_page` parameter to limit the number of pages to fetch. For example:

    client = RESTClient(
        base_url="https://api.example.com",
        paginator=PageNumberPaginator(
            maximum_page=5,
            total_path=None
        )
    )
    ...

In this case, pagination will stop after fetching 5 pages of data.

### \_\_init\_\_

```python
def __init__(base_page: int = 0,
             page: int = None,
             page_param: str = "page",
             total_path: jsonpath.TJsonPath = "total",
             maximum_page: Optional[int] = None,
             stop_after_empty_page: Optional[bool] = True)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/helpers/rest_client/paginators.py#L242)

**Arguments**:

- `base_page` _int_ - The index of the initial page from the API perspective.
  Determines the page number that the API server uses for the starting
  page. Normally, this is 0-based or 1-based (e.g., 1, 2, 3, ...)
  indexing for the pages. Defaults to 0.
- `page` _int_ - The page number for the first request. If not provided,
  the initial value will be set to `base_page`.
- `page_param` _str_ - The query parameter name for the page number.
  Defaults to 'page'.
- `total_path` _jsonpath.TJsonPath_ - The JSONPath expression for
  the total number of pages. Defaults to 'total'.
- `maximum_page` _int_ - The maximum page number. If provided, pagination
  will stop once this page is reached or exceeded, even if more
  data is available. This allows you to limit the maximum number
  of pages for pagination. Defaults to None.
- `stop_after_empty_page` _bool_ - Whether pagination should stop when
  a page contains no result items. Defaults to `True`.

## OffsetPaginator Objects

```python
class OffsetPaginator(RangePaginator)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/helpers/rest_client/paginators.py#L296)

A paginator that uses offset-based pagination strategy.

This paginator is useful for APIs where pagination is controlled
through offset and limit query parameters and the total count of items
is returned in the response.

For example, consider an API located at `https://api.example.com/items`
that supports pagination through offset and limit, and provides the total
item count in its responses, as shown below:

    {
        "items": [...],
        "total": 1000
    }

To use `OffsetPaginator` with such an API, you can instantiate `RESTClient`
as follows:

    from dlt.sources.helpers.rest_client import RESTClient

    client = RESTClient(
        base_url="https://api.example.com",
        paginator=OffsetPaginator(
            limit=100,
            total_path="total"
        )
    )
    @dlt.resource
    def get_items():
        for page in client.paginate("/items"):
            yield page

The `OffsetPaginator` will automatically increment the offset for each
subsequent request until all items are fetched.

If the API does not provide the total count of items, you can use the
`maximum_offset` parameter to limit the number of items to fetch. For example:

    client = RESTClient(
        base_url="https://api.example.com",
        paginator=OffsetPaginator(
            limit=100,
            maximum_offset=1000,
            total_path=None
        )
    )
    ...

In this case, pagination will stop after fetching 1000 items.

### \_\_init\_\_

```python
def __init__(limit: int,
             offset: int = 0,
             offset_param: str = "offset",
             limit_param: str = "limit",
             total_path: jsonpath.TJsonPath = "total",
             maximum_offset: Optional[int] = None,
             stop_after_empty_page: Optional[bool] = True) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/helpers/rest_client/paginators.py#L348)

**Arguments**:

- `limit` _int_ - The maximum number of items to retrieve
  in each request.
- `offset` _int_ - The offset for the first request.
  Defaults to 0.
- `offset_param` _str_ - The query parameter name for the offset.
  Defaults to 'offset'.
- `limit_param` _str_ - The query parameter name for the limit.
  Defaults to 'limit'.
- `total_path` _jsonpath.TJsonPath_ - The JSONPath expression for
  the total number of items.
- `maximum_offset` _int_ - The maximum offset value. If provided,
  pagination will stop once this offset is reached or exceeded,
  even if more data is available. This allows you to limit the
  maximum range for pagination. Defaults to None.
- `stop_after_empty_page` _bool_ - Whether pagination should stop when
  a page contains no result items. Defaults to `True`.

## BaseReferencePaginator Objects

```python
class BaseReferencePaginator(BasePaginator)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/helpers/rest_client/paginators.py#L410)

A base paginator class for paginators that use a reference to the next
page, such as a URL or a cursor string.

Subclasses should implement:
  1. `update_state` method to extract the next page reference and
    set the `_next_reference` attribute accordingly.
  2. `update_request` method to update the request object with the next
    page reference.

## BaseNextUrlPaginator Objects

```python
class BaseNextUrlPaginator(BaseReferencePaginator)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/helpers/rest_client/paginators.py#L447)

A base paginator class for paginators that use a URL provided in the API
response to fetch the next page. For example, the URL can be found in HTTP
headers or in the JSON response.

Subclasses should implement the `update_state` method to extract the next
page URL and set the `_next_reference` attribute accordingly.

See `HeaderLinkPaginator` and `JSONLinkPaginator` for examples.

## HeaderLinkPaginator Objects

```python
class HeaderLinkPaginator(BaseNextUrlPaginator)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/helpers/rest_client/paginators.py#L473)

A paginator that uses the 'Link' header in HTTP responses
for pagination.

A good example of this is the GitHub API:
    https://docs.github.com/en/rest/guides/traversing-with-pagination

For example, consider an API response that includes 'Link' header:

    ...
    Content-Type: application/json
    Link: <https://api.example.com/items?page=2>; rel="next", <https://api.example.com/items?page=1>; rel="prev"

    [
        {"id": 1, "name": "item1"},
        {"id": 2, "name": "item2"},
        ...
    ]

In this scenario, the URL for the next page (`https://api.example.com/items?page=2`)
is identified by its relation type `rel="next"`. `HeaderLinkPaginator` extracts
this URL from the 'Link' header and uses it to fetch the next page of results:

    from dlt.sources.helpers.rest_client import RESTClient
    client = RESTClient(
        base_url="https://api.example.com",
        paginator=HeaderLinkPaginator()
    )

    @dlt.resource
    def get_issues():
        for page in client.paginate("/items"):
            yield page

### \_\_init\_\_

```python
def __init__(links_next_key: str = "next") -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/helpers/rest_client/paginators.py#L508)

**Arguments**:

- `links_next_key` _str, optional_ - The key (rel) in the 'Link' header
  that contains the next page URL. Defaults to 'next'.

### update\_state

```python
def update_state(response: Response, data: Optional[List[Any]] = None) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/helpers/rest_client/paginators.py#L517)

Extracts the next page URL from the 'Link' header in the response.

## JSONLinkPaginator Objects

```python
class JSONLinkPaginator(BaseNextUrlPaginator)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/helpers/rest_client/paginators.py#L525)

Locates the next page URL within the JSON response body. The key
containing the URL can be specified using a JSON path.

For example, suppose the JSON response from an API contains data items
along with a 'pagination' object:

    {
        "items": [
            {"id": 1, "name": "item1"},
            {"id": 2, "name": "item2"},
            ...
        ],
        "pagination": {
            "next": "https://api.example.com/items?page=2"
        }
    }

The link to the next page (`https://api.example.com/items?page=2`) is
located in the 'next' key of the 'pagination' object. You can use
`JSONLinkPaginator` to paginate through the API endpoint:

    from dlt.sources.helpers.rest_client import RESTClient
    client = RESTClient(
        base_url="https://api.example.com",
        paginator=JSONLinkPaginator(next_url_path="pagination.next")
    )

    @dlt.resource
    def get_data():
        for page in client.paginate("/posts"):
            yield page

### \_\_init\_\_

```python
def __init__(next_url_path: jsonpath.TJsonPath = "next")
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/helpers/rest_client/paginators.py#L559)

**Arguments**:

- `next_url_path` _jsonpath.TJsonPath_ - The JSON path to the key
  containing the next page URL in the response body.
  Defaults to 'next'.

### update\_state

```python
def update_state(response: Response, data: Optional[List[Any]] = None) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/helpers/rest_client/paginators.py#L572)

Extracts the next page URL from the JSON response.

## JSONResponseCursorPaginator Objects

```python
class JSONResponseCursorPaginator(BaseReferencePaginator)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/helpers/rest_client/paginators.py#L595)

Uses a cursor parameter for pagination, with the cursor value found in
the JSON response body.

For example, suppose the JSON response from an API contains
a 'cursors' object:

    {
        "items": [
            {"id": 1, "name": "item1"},
            {"id": 2, "name": "item2"},
            ...
        ],
        "cursors": {
            "next": "aW1wb3J0IGFudGlncmF2aXR5"
        }
    }

And the API endpoint expects a 'cursor' query parameter to fetch
the next page. So the URL for the next page would look
like `https://api.example.com/items?cursor=aW1wb3J0IGFudGlncmF2aXR5`.

You can paginate through this API endpoint using
`JSONResponseCursorPaginator`:

    from dlt.sources.helpers.rest_client import RESTClient
    client = RESTClient(
        base_url="https://api.example.com",
        paginator=JSONResponseCursorPaginator(
            cursor_path="cursors.next",
            cursor_param="cursor"
        )
    )

    @dlt.resource
    def get_data():
        for page in client.paginate("/posts"):
            yield page

### \_\_init\_\_

```python
def __init__(cursor_path: jsonpath.TJsonPath = "cursors.next",
             cursor_param: str = "cursor")
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/helpers/rest_client/paginators.py#L635)

**Arguments**:

- `cursor_path` - The JSON path to the key that contains the cursor in
  the response.
- `cursor_param` - The name of the query parameter to be used in
  the request to get the next page.

### update\_state

```python
def update_state(response: Response, data: Optional[List[Any]] = None) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/helpers/rest_client/paginators.py#L651)

Extracts the cursor value from the JSON response.

### update\_request

```python
def update_request(request: Request) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/helpers/rest_client/paginators.py#L656)

Updates the request with the cursor query parameter.

