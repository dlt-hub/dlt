---
sidebar_label: auth
title: sources.helpers.rest_client.auth
---

## TApiKeyLocation

Alias for scheme "in" field

## AuthConfigBase Objects

```python
class AuthConfigBase(AuthBase, CredentialsConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/sources/helpers/rest_client/auth.py#L38)

Authenticator base which is both `requests` friendly AuthBase and dlt SPEC
configurable via env variables or toml files

## BearerTokenAuth Objects

```python
@configspec
class BearerTokenAuth(AuthConfigBase)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/sources/helpers/rest_client/auth.py#L51)

Uses `token` for Bearer authentication in "Authorization" header.

## APIKeyAuth Objects

```python
@configspec
class APIKeyAuth(AuthConfigBase)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/sources/helpers/rest_client/auth.py#L72)

Uses provided `api_key` to create authorization data in the specified `location` (query, param, header, cookie) under specified `name`

## HttpBasicAuth Objects

```python
@configspec
class HttpBasicAuth(AuthConfigBase)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/sources/helpers/rest_client/auth.py#L100)

Uses HTTP basic authentication

## OAuth2AuthBase Objects

```python
@configspec
class OAuth2AuthBase(AuthConfigBase)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/sources/helpers/rest_client/auth.py#L126)

Base class for oauth2 authenticators. requires access_token

## OAuth2ClientCredentials Objects

```python
@configspec
class OAuth2ClientCredentials(OAuth2AuthBase)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/sources/helpers/rest_client/auth.py#L148)

This class implements OAuth2 Client Credentials flow where the autorization service
gives permission without the end user approving.
This is often used for machine-to-machine authorization.
The client sends its client ID and client secret to the authorization service which replies
with a temporary access token.
With the access token, the client can access resource services.

## OAuthJWTAuth Objects

```python
@configspec
class OAuthJWTAuth(BearerTokenAuth)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/sources/helpers/rest_client/auth.py#L218)

This is a form of Bearer auth, actually there's not standard way to declare it in openAPI

