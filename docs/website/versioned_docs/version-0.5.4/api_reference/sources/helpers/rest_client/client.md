---
sidebar_label: client
title: sources.helpers.rest_client.client
---

## PageData Objects

```python
class PageData(List[_T])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/sources/helpers/rest_client/client.py#L32)

A list of elements in a single page of results with attached request context.

The context allows to inspect the response, paginator and authenticator, modify the request

## RESTClient Objects

```python
class RESTClient()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/sources/helpers/rest_client/client.py#L53)

A generic REST client for making requests to an API with support for
pagination and authentication.

**Arguments**:

- `base_url` _str_ - The base URL of the API to make requests to.
- `headers` _Optional[Dict[str, str]]_ - Default headers to include in all requests.
- `auth` _Optional[AuthBase]_ - Authentication configuration for all requests.
- `paginator` _Optional[BasePaginator]_ - Default paginator for handling paginated responses.
- `data_selector` _Optional[jsonpath.TJsonPath]_ - JSONPath selector for extracting data from responses.
- `session` _BaseSession_ - HTTP session for making requests.
- `paginator_factory` _Optional[PaginatorFactory]_ - Factory for creating paginator instances,
  used for detecting paginators.

### paginate

```python
def paginate(path: str = "",
             method: HTTPMethodBasic = "GET",
             params: Optional[Dict[str, Any]] = None,
             json: Optional[Dict[str, Any]] = None,
             auth: Optional[AuthBase] = None,
             paginator: Optional[BasePaginator] = None,
             data_selector: Optional[jsonpath.TJsonPath] = None,
             hooks: Optional[Hooks] = None,
             **kwargs: Any) -> Iterator[PageData[Any]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/sources/helpers/rest_client/client.py#L155)

Iterates over paginated API responses, yielding pages of data.

**Arguments**:

- `path` _str_ - Endpoint path for the request, relative to `base_url`.
- `method` _HTTPMethodBasic_ - HTTP method for the request, defaults to 'get'.
- `params` _Optional[Dict[str, Any]]_ - URL parameters for the request.
- `json` _Optional[Dict[str, Any]]_ - JSON payload for the request.
- `auth` _Optional[AuthBase_ - Authentication configuration for the request.
- `paginator` _Optional[BasePaginator]_ - Paginator instance for handling
  pagination logic.
- `data_selector` _Optional[jsonpath.TJsonPath]_ - JSONPath selector for
  extracting data from the response.
- `hooks` _Optional[Hooks]_ - Hooks to modify request/response objects. Note that
  when hooks are not provided, the default behavior is to raise an exception
  on error status codes.
- `**kwargs` _Any_ - Optional arguments to that the Request library accepts, such as
  `stream`, `verify`, `proxies`, `cert`, `timeout`, and `allow_redirects`.
  
  

**Yields**:

- `PageData[Any]` - A page of data from the paginated API response, along with request and response context.
  

**Raises**:

- `HTTPError` - If the response status code is not a success code. This is raised
  by default when hooks are not provided.
  

**Example**:

```py
    client = RESTClient(base_url="https://api.example.com")
    for page in client.paginate("/search", method="post", json={"query": "foo"}):
        print(page)
```

### detect\_data\_selector

```python
def detect_data_selector(response: Response) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/sources/helpers/rest_client/client.py#L255)

Detects a path to page data in `response`. If there's no
paging detected, returns "$" which will select full response

**Returns**:

- `str` - a json path to the page data.

### detect\_paginator

```python
def detect_paginator(response: Response, data: Any) -> BasePaginator
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/sources/helpers/rest_client/client.py#L273)

Detects a paginator for the response and returns it.

**Arguments**:

- `response` _Response_ - The response to detect the paginator for.
- `data_selector` _data_selector_ - Path to paginated data or $ if paginated data not detected
  

**Returns**:

- `BasePaginator` - The paginator instance that was detected.

