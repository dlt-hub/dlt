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

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/helpers/rest_client/__init__.py#L12)

Paginate over a REST API endpoint.

**Arguments**:

- `url` - URL to paginate over.
- `**kwargs` - Keyword arguments to pass to `RESTClient.paginate`.
  

**Returns**:

- `Iterator[Page]` - Iterator over pages.

