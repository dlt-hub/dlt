---
sidebar_label: gcp_credentials
title: common.configuration.specs.gcp_credentials
---

## GcpCredentials Objects

```python
@configspec
class GcpCredentials(CredentialsConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/specs/gcp_credentials.py#L15)

#### location

DEPRECATED! and present only for backward compatibility. please set bigquery location in BigQuery configuration

#### to\_native\_credentials

```python
def to_native_credentials() -> Any
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/specs/gcp_credentials.py#L30)

Returns respective native credentials for service account or oauth2 that can be passed to google clients

## GcpServiceAccountCredentialsWithoutDefaults Objects

```python
@configspec
class GcpServiceAccountCredentialsWithoutDefaults(GcpCredentials)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/specs/gcp_credentials.py#L42)

#### type

noqa: A003

#### parse\_native\_representation

```python
def parse_native_representation(native_value: Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/specs/gcp_credentials.py#L47)

Accepts ServiceAccountCredentials as native value. In other case reverts to serialized services.json

#### to\_native\_credentials

```python
def to_native_credentials() -> Any
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/specs/gcp_credentials.py#L83)

Returns google.oauth2.service_account.Credentials

## GcpOAuthCredentialsWithoutDefaults Objects

```python
@configspec
class GcpOAuthCredentialsWithoutDefaults(GcpCredentials, OAuth2Credentials)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/specs/gcp_credentials.py#L98)

#### parse\_native\_representation

```python
def parse_native_representation(native_value: Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/specs/gcp_credentials.py#L103)

Accepts Google OAuth2 credentials as native value. In other case reverts to serialized oauth client secret json

#### on\_partial

```python
def on_partial() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/specs/gcp_credentials.py#L151)

Allows for an empty refresh token if the session is interactive or tty is attached

#### to\_native\_credentials

```python
def to_native_credentials() -> Any
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/specs/gcp_credentials.py#L183)

Returns google.oauth2.credentials.Credentials

## GcpDefaultCredentials Objects

```python
@configspec
class GcpDefaultCredentials(CredentialsWithDefault, GcpCredentials)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/specs/gcp_credentials.py#L213)

#### parse\_native\_representation

```python
def parse_native_representation(native_value: Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/specs/gcp_credentials.py#L217)

Accepts google credentials as native value

#### on\_partial

```python
def on_partial() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/specs/gcp_credentials.py#L248)

Looks for default google credentials and resolves configuration if found. Otherwise continues as partial

