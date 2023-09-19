---
sidebar_label: aws_credentials
title: common.configuration.specs.aws_credentials
---

## AwsCredentialsWithoutDefaults Objects

```python
@configspec
class AwsCredentialsWithoutDefaults(CredentialsConfiguration)
```

#### to\_s3fs\_credentials

```python
def to_s3fs_credentials() -> Dict[str, Optional[str]]
```

Dict of keyword arguments that can be passed to s3fs

#### to\_native\_representation

```python
def to_native_representation() -> Dict[str, Optional[str]]
```

Return a dict that can be passed as kwargs to boto3 session

## AwsCredentials Objects

```python
@configspec
class AwsCredentials(AwsCredentialsWithoutDefaults, CredentialsWithDefault)
```

#### parse\_native\_representation

```python
def parse_native_representation(native_value: Any) -> None
```

Import external boto3 session

