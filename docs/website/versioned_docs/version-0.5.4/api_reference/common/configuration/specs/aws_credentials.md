---
sidebar_label: aws_credentials
title: common.configuration.specs.aws_credentials
---

## AwsCredentialsWithoutDefaults Objects

```python
@configspec
class AwsCredentialsWithoutDefaults(CredentialsConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/specs/aws_credentials.py#L19)

### to\_s3fs\_credentials

```python
def to_s3fs_credentials() -> Dict[str, Optional[str]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/specs/aws_credentials.py#L28)

Dict of keyword arguments that can be passed to s3fs

### to\_native\_representation

```python
def to_native_representation() -> Dict[str, Optional[str]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/specs/aws_credentials.py#L41)

Return a dict that can be passed as kwargs to boto3 session

## AwsCredentials Objects

```python
@configspec
class AwsCredentials(AwsCredentialsWithoutDefaults, CredentialsWithDefault)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/specs/aws_credentials.py#L80)

### to\_session\_credentials

```python
def to_session_credentials() -> Dict[str, str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/specs/aws_credentials.py#L87)

Return configured or new aws session token

### parse\_native\_representation

```python
def parse_native_representation(native_value: Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/specs/aws_credentials.py#L147)

Import external boto3 session

