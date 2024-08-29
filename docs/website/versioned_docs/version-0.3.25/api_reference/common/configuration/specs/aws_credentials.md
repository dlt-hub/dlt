---
sidebar_label: aws_credentials
title: common.configuration.specs.aws_credentials
---

## AwsCredentialsWithoutDefaults Objects

```python
@configspec
class AwsCredentialsWithoutDefaults(CredentialsConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/specs/aws_credentials.py#L15)

### to\_s3fs\_credentials

```python
def to_s3fs_credentials() -> Dict[str, Optional[str]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/specs/aws_credentials.py#L24)

Dict of keyword arguments that can be passed to s3fs

### to\_native\_representation

```python
def to_native_representation() -> Dict[str, Optional[str]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/specs/aws_credentials.py#L37)

Return a dict that can be passed as kwargs to boto3 session

## AwsCredentials Objects

```python
@configspec
class AwsCredentials(AwsCredentialsWithoutDefaults, CredentialsWithDefault)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/specs/aws_credentials.py#L50)

### to\_session\_credentials

```python
def to_session_credentials() -> Dict[str, str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/specs/aws_credentials.py#L57)

Return configured or new aws session token

### parse\_native\_representation

```python
def parse_native_representation(native_value: Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/specs/aws_credentials.py#L117)

Import external boto3 session

