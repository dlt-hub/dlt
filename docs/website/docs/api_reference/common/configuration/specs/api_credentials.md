---
sidebar_label: api_credentials
title: common.configuration.specs.api_credentials
---

## OAuth2Credentials Objects

```python
@configspec
class OAuth2Credentials(CredentialsConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/specs/api_credentials.py#L8)

#### token

Access token

#### auth

```python
def auth(scopes: Union[str, List[str]] = None,
         redirect_url: str = None) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/specs/api_credentials.py#L21)

Authorizes the client using the available credentials

Uses the `refresh_token` grant if refresh token is available. Note that `scopes` and `redirect_url` are ignored in this flow.
Otherwise obtains refresh_token via web flow and authorization code grant.

Sets `token` and `access_token` fields in the credentials on successful authorization.

**Arguments**:

- `scopes` _Union[str, List[str]], optional_ - Additional scopes to add to configured scopes. To be used in web flow. Defaults to None.
- `redirect_url` _str, optional_ - Redirect url in case of web flow. Defaults to None.

