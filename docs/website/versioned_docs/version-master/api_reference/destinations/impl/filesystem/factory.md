---
sidebar_label: factory
title: destinations.impl.filesystem.factory
---

## filesystem Objects

```python
class filesystem(Destination[FilesystemDestinationClientConfiguration,
                             "FilesystemClient"])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/filesystem/factory.py#L39)

### \_\_init\_\_

```python
def __init__(bucket_url: str = None,
             credentials: t.Union[FileSystemCredentials, t.Dict[str, t.Any],
                                  t.Any] = None,
             layout: str = DEFAULT_FILE_LAYOUT,
             extra_placeholders: t.Optional[TExtraPlaceholders] = None,
             current_datetime: t.Optional[TCurrentDateTime] = None,
             destination_name: t.Optional[str] = None,
             environment: t.Optional[str] = None,
             **kwargs: t.Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/filesystem/factory.py#L63)

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
- `layout` _str_ - A layout of the files holding table data in the destination bucket/filesystem. Uses a set of pre-defined
  and user-defined (extra) placeholders. Please refer to https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem#files-layout
  extra_placeholders (dict(str, str | callable)): A dictionary of extra placeholder names that can be used in the `layout` parameter. Names
  are mapped to string values or to callables evaluated at runtime.
- `current_datetime` _DateTime | callable_ - current datetime used by date/time related placeholders. If not provided, load package creation timestamp
  will be used.
- `**kwargs` - Additional arguments passed to the destination config

