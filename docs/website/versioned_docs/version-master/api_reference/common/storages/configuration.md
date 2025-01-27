---
sidebar_label: configuration
title: common.storages.configuration
---

## SchemaStorageConfiguration Objects

```python
@configspec
class SchemaStorageConfiguration(BaseConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/configuration.py#L27)

### schema\_volume\_path

path to volume with default schemas

### import\_schema\_path

path from which to import a schema into storage

### export\_schema\_path

path to which export schema from storage

### external\_schema\_format

format in which to expect external schema

## NormalizeStorageConfiguration Objects

```python
@configspec
class NormalizeStorageConfiguration(BaseConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/configuration.py#L38)

### normalize\_volume\_path

path to volume where normalized loader files will be stored

## ensure\_canonical\_az\_url

```python
def ensure_canonical_az_url(bucket_url: str,
                            target_scheme: str,
                            storage_account_name: str = None,
                            account_host: str = None) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/configuration.py#L61)

Converts any of the forms of azure blob storage into canonical form of {target_scheme}://<container_name>@<storage_account_name>.{account_host}/path

`azure_storage_account_name` is optional only if not present in bucket_url, `account_host` assumes "<azure_storage_account_name>.dfs.core.windows.net" by default

## make\_fsspec\_url

```python
def make_fsspec_url(scheme: str, fs_path: str, bucket_url: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/configuration.py#L136)

Creates url from `fs_path` and `scheme` using bucket_url as an `url` template

**Arguments**:

- `scheme` _str_ - scheme of the resulting url
- `fs_path` _str_ - kind of absolute path that fsspec uses to locate resources for particular filesystem.
- `bucket_url` _str_ - an url template. the structure of url will be preserved if possible

## FilesystemConfiguration Objects

```python
@configspec
class FilesystemConfiguration(BaseConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/configuration.py#L151)

A configuration defining filesystem location and access credentials.

When configuration is resolved, `bucket_url` is used to extract a protocol and request corresponding credentials class.
* s3
* gs, gcs
* az, abfs, adl, abfss, azure
* file, memory
* gdrive
* sftp

### read\_only

Indicates read only filesystem access. Will enable caching

### max\_state\_files

Maximum number of pipeline state files to keep; 0 or negative value disables cleanup.

### protocol

```python
@property
def protocol() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/configuration.py#L190)

`bucket_url` protocol

### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/configuration.py#L218)

Returns a fingerprint of bucket schema and netloc.

**Returns**:

- `str` - Fingerprint.

### make\_url

```python
def make_url(fs_path: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/configuration.py#L233)

Makes a full url (with scheme) form fs_path which is kind-of absolute path used by fsspec to identify resources.
This method will use `bucket_url` to infer the original form of the url.

### \_\_str\_\_

```python
def __str__() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/configuration.py#L239)

Return displayable destination location

### is\_local\_path

```python
@staticmethod
def is_local_path(url: str) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/configuration.py#L251)

Checks if `url` is a local path, without a schema

### make\_local\_path

```python
@staticmethod
def make_local_path(file_url: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/configuration.py#L258)

Gets a valid local filesystem path from file:// scheme.
Supports POSIX/Windows/UNC paths

**Returns**:

- `str` - local filesystem path

### make\_file\_url

```python
@staticmethod
def make_file_url(local_path: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/configuration.py#L286)

Creates a normalized file:// url from a local path

netloc is never set. UNC paths are represented as file://host/path

