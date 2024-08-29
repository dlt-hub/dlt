---
sidebar_label: gcp_credentials
title: common.configuration.specs.gcp_credentials
---

## GcpCredentials Objects

```python
@configspec
class GcpCredentials(CredentialsConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/specs/gcp_credentials.py#L24)

### to\_native\_credentials

```python
def to_native_credentials() -> Any
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/specs/gcp_credentials.py#L43)

Returns respective native credentials for service account or oauth2 that can be passed to google clients

### to\_gcs\_credentials

```python
def to_gcs_credentials() -> Dict[str, Any]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/specs/gcp_credentials.py#L53)

Dict of keyword arguments can be passed to gcsfs.
Delegates default GCS credential handling to gcsfs.

## GcpServiceAccountCredentialsWithoutDefaults Objects

```python
@configspec
class GcpServiceAccountCredentialsWithoutDefaults(GcpCredentials)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/specs/gcp_credentials.py#L69)

### type

noqa: A003

### parse\_native\_representation

```python
def parse_native_representation(native_value: Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/specs/gcp_credentials.py#L74)

Accepts ServiceAccountCredentials as native value. In other case reverts to serialized services.json

### to\_native\_credentials

```python
def to_native_credentials() -> Any
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/specs/gcp_credentials.py#L107)

Returns google.oauth2.service_account.Credentials

## GcpOAuthCredentialsWithoutDefaults Objects

```python
@configspec
class GcpOAuthCredentialsWithoutDefaults(GcpCredentials, OAuth2Credentials)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/specs/gcp_credentials.py#L122)

### parse\_native\_representation

```python
def parse_native_representation(native_value: Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/specs/gcp_credentials.py#L127)

Accepts Google OAuth2 credentials as native value. In other case reverts to serialized oauth client secret json

### on\_partial

```python
def on_partial() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/specs/gcp_credentials.py#L180)

Allows for an empty refresh token if the session is interactive or tty is attached

### to\_native\_credentials

```python
def to_native_credentials() -> Any
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/specs/gcp_credentials.py#L211)

Returns google.oauth2.credentials.Credentials

## GcpDefaultCredentials Objects

```python
@configspec
class GcpDefaultCredentials(CredentialsWithDefault, GcpCredentials)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/specs/gcp_credentials.py#L239)

### parse\_native\_representation

```python
def parse_native_representation(native_value: Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/specs/gcp_credentials.py#L242)

Accepts google credentials as native value

### on\_partial

```python
def on_partial() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/specs/gcp_credentials.py#L275)

Looks for default google credentials and resolves configuration if found. Otherwise continues as partial

