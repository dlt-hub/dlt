---
sidebar_label: typing
title: sources.rest_api.typing
---

## PaginatorTypeConfig Objects

```python
class PaginatorTypeConfig(TypedDict)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/rest_api/typing.py#L79)

### type

noqa

## PageNumberPaginatorConfig Objects

```python
class PageNumberPaginatorConfig(PaginatorTypeConfig)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/rest_api/typing.py#L83)

A paginator that uses page number-based pagination strategy.

## OffsetPaginatorConfig Objects

```python
class OffsetPaginatorConfig(PaginatorTypeConfig)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/rest_api/typing.py#L92)

A paginator that uses offset-based pagination strategy.

## HeaderLinkPaginatorConfig Objects

```python
class HeaderLinkPaginatorConfig(PaginatorTypeConfig)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/rest_api/typing.py#L103)

A paginator that uses the 'Link' header in HTTP responses
for pagination.

## JSONLinkPaginatorConfig Objects

```python
class JSONLinkPaginatorConfig(PaginatorTypeConfig)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/rest_api/typing.py#L110)

Locates the next page URL within the JSON response body. The key
containing the URL can be specified using a JSON path.

## JSONResponseCursorPaginatorConfig Objects

```python
class JSONResponseCursorPaginatorConfig(PaginatorTypeConfig)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/rest_api/typing.py#L117)

Uses a cursor parameter for pagination, with the cursor value found in
the JSON response body.

## AuthTypeConfig Objects

```python
class AuthTypeConfig(TypedDict)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/rest_api/typing.py#L145)

### type

noqa

## BearerTokenAuthConfig Objects

```python
class BearerTokenAuthConfig(TypedDict)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/rest_api/typing.py#L149)

Uses `token` for Bearer authentication in "Authorization" header.

### type

noqa

## ApiKeyAuthConfig Objects

```python
class ApiKeyAuthConfig(AuthTypeConfig)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/rest_api/typing.py#L157)

Uses provided `api_key` to create authorization data in the specified `location` (query, param, header, cookie) under specified `name`

## HttpBasicAuthConfig Objects

```python
class HttpBasicAuthConfig(AuthTypeConfig)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/rest_api/typing.py#L165)

Uses HTTP basic authentication

## OAuth2ClientCredentialsConfig Objects

```python
class OAuth2ClientCredentialsConfig(AuthTypeConfig)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/rest_api/typing.py#L172)

Uses OAuth 2.0 client credential authorization

## ParamBindConfig Objects

```python
class ParamBindConfig(TypedDict)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/rest_api/typing.py#L222)

### type

noqa

## ProcessingSteps Objects

```python
class ProcessingSteps(TypedDict)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/rest_api/typing.py#L268)

### filter

noqa: A003

### map

noqa: A003

## ResourceBase Objects

```python
class ResourceBase(TResourceHintsBase)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/rest_api/typing.py#L273)

Defines hints that may be passed to `dlt.resource` decorator

