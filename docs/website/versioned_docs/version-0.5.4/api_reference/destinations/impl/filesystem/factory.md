---
sidebar_label: factory
title: destinations.impl.filesystem.factory
---

## filesystem Objects

```python
class filesystem(Destination[FilesystemDestinationClientConfiguration,
                             "FilesystemClient"])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/filesystem/factory.py#L12)

### \_\_init\_\_

```python
def __init__(bucket_url: str = None,
             credentials: t.Union[FileSystemCredentials, t.Dict[str, t.Any],
                                  t.Any] = None,
             destination_name: t.Optional[str] = None,
             environment: t.Optional[str] = None,
             **kwargs: t.Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/filesystem/factory.py#L24)

Configure the filesystem destination to use in a pipeline and load data to local or remote filesystem.

All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

The `bucket_url` determines the protocol to be used:

- Local folder: `file:///path/to/directory`
- AWS S3 (and S3 compatible storages): `s3://bucket-name
- Azure Blob Storage: `az://container-name
- Google Cloud Storage: `gs://bucket-name
- Memory fs: `memory://m`

**Arguments**:

- `bucket_url` - The fsspec compatible bucket url to use for the destination.
- `credentials` - Credentials to connect to the filesystem. The type of credentials should correspond to
  the bucket protocol. For example, for AWS S3, the credentials should be an instance of `AwsCredentials`.
  A dictionary with the credentials parameters can also be provided.
- `**kwargs` - Additional arguments passed to the destination config

