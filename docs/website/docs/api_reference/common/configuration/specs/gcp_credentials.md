---
sidebar_label: gcp_credentials
title: common.configuration.specs.gcp_credentials
---

## GcpCredentials Objects

```python
@configspec
class GcpCredentials(CredentialsConfiguration)
```

#### location

DEPRECATED! and present only for backward compatibility. please set bigquery location in BigQuery configuration

#### to\_native\_credentials

```python
def to_native_credentials() -> Any
```

Returns respective native credentials for service account or oauth2 that can be passed to google clients

## GcpServiceAccountCredentialsWithoutDefaults Objects

```python
@configspec
class GcpServiceAccountCredentialsWithoutDefaults(GcpCredentials)
```

#### type

noqa: A003

#### parse\_native\_representation

```python
def parse_native_representation(native_value: Any) -> None
```

Accepts ServiceAccountCredentials as native value. In other case reverts to serialized services.json

#### to\_native\_credentials

```python
def to_native_credentials() -> Any
```

Returns google.oauth2.service_account.Credentials

## GcpOAuthCredentialsWithoutDefaults Objects

```python
@configspec
class GcpOAuthCredentialsWithoutDefaults(GcpCredentials, OAuth2Credentials)
```

#### parse\_native\_representation

```python
def parse_native_representation(native_value: Any) -> None
```

Accepts Google OAuth2 credentials as native value. In other case reverts to serialized oauth client secret json

#### on\_partial

```python
def on_partial() -> None
```

Allows for an empty refresh token if the session is interactive or tty is attached

#### to\_native\_credentials

```python
def to_native_credentials() -> Any
```

Returns google.oauth2.credentials.Credentials

## GcpDefaultCredentials Objects

```python
@configspec
class GcpDefaultCredentials(CredentialsWithDefault, GcpCredentials)
```

#### parse\_native\_representation

```python
def parse_native_representation(native_value: Any) -> None
```

Accepts google credentials as native value

#### on\_partial

```python
def on_partial() -> None
```

Looks for default google credentials and resolves configuration if found. Otherwise continues as partial

