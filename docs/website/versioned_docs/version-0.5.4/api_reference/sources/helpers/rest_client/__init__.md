---
sidebar_label: rest_client
title: sources.helpers.rest_client
---

## paginate

```python
def paginate(url: str,
             method: HTTPMethodBasic = "GET",
             headers: Optional[Dict[str, str]] = None,
             params: Optional[Dict[str, Any]] = None,
             json: Optional[Dict[str, Any]] = None,
             auth: AuthConfigBase = None,
             paginator: Optional[BasePaginator] = None,
             data_selector: Optional[jsonpath.TJsonPath] = None,
             hooks: Optional[Hooks] = None) -> Iterator[PageData[Any]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/sources/helpers/rest_client/__init__.py#L12)

Paginate over a REST API endpoint.

**Arguments**:

- `url` - URL to paginate over.
- `**kwargs` - Keyword arguments to pass to `RESTClient.paginate`.
  

**Returns**:

- `Iterator[Page]` - Iterator over pages.

