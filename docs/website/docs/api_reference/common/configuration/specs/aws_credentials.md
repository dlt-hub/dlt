---
sidebar_label: aws_credentials
title: common.configuration.specs.aws_credentials
---

## AwsCredentialsWithoutDefaults Objects

```python
@configspec
class AwsCredentialsWithoutDefaults(CredentialsConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/specs/aws_credentials.py#L11)

#### to\_s3fs\_credentials

```python
def to_s3fs_credentials() -> Dict[str, Optional[str]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/specs/aws_credentials.py#L19)

Dict of keyword arguments that can be passed to s3fs

#### to\_native\_representation

```python
def to_native_representation() -> Dict[str, Optional[str]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/specs/aws_credentials.py#L28)

Return a dict that can be passed as kwargs to boto3 session

## AwsCredentials Objects

```python
@configspec
class AwsCredentials(AwsCredentialsWithoutDefaults, CredentialsWithDefault)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/specs/aws_credentials.py#L34)

#### parse\_native\_representation

```python
def parse_native_representation(native_value: Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/specs/aws_credentials.py#L82)

Import external boto3 session

