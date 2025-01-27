---
sidebar_label: gcp_credentials
title: common.configuration.specs.gcp_credentials
---

## GcpCredentials Objects

```python
@configspec
class GcpCredentials(CredentialsConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/specs/gcp_credentials.py#L26)

### to\_native\_credentials

```python
def to_native_credentials() -> Any
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/specs/gcp_credentials.py#L43)

Returns respective native credentials for service account or oauth2 that can be passed to google clients

### to\_gcs\_credentials

```python
def to_gcs_credentials() -> Dict[str, Any]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/specs/gcp_credentials.py#L53)

Dict of keyword arguments that can be passed to gcsfs.
Delegates default GCS credential handling to gcsfs.

### to\_object\_store\_rs\_credentials

```python
def to_object_store_rs_credentials() -> Dict[str, str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/specs/gcp_credentials.py#L67)

Dict of keyword arguments that can be passed to `object_store` Rust crate.
Delegates default GCS credential handling to `object_store` Rust crate.

## GcpServiceAccountCredentialsWithoutDefaults Objects

```python
@configspec
class GcpServiceAccountCredentialsWithoutDefaults(GcpCredentials)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/specs/gcp_credentials.py#L78)

### parse\_native\_representation

```python
def parse_native_representation(native_value: Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/specs/gcp_credentials.py#L86)

Accepts ServiceAccountCredentials as native value. In other case reverts to serialized services.json

### to\_native\_credentials

```python
def to_native_credentials() -> Any
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/specs/gcp_credentials.py#L119)

Returns google.oauth2.service_account.Credentials

## GcpOAuthCredentialsWithoutDefaults Objects

```python
@configspec
class GcpOAuthCredentialsWithoutDefaults(GcpCredentials, OAuth2Credentials)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/specs/gcp_credentials.py#L134)

### parse\_native\_representation

```python
def parse_native_representation(native_value: Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/specs/gcp_credentials.py#L141)

Accepts Google OAuth2 credentials as native value. In other case reverts to serialized oauth client secret json

### on\_partial

```python
def on_partial() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/specs/gcp_credentials.py#L200)

Allows for an empty refresh token if the session is interactive or tty is attached

### to\_native\_credentials

```python
def to_native_credentials() -> Any
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/specs/gcp_credentials.py#L231)

Returns google.oauth2.credentials.Credentials

## GcpDefaultCredentials Objects

```python
@configspec
class GcpDefaultCredentials(CredentialsWithDefault, GcpCredentials)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/specs/gcp_credentials.py#L259)

### parse\_native\_representation

```python
def parse_native_representation(native_value: Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/specs/gcp_credentials.py#L262)

Accepts google credentials as native value

### on\_partial

```python
def on_partial() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/specs/gcp_credentials.py#L295)

Looks for default google credentials and resolves configuration if found. Otherwise continues as partial

