---
sidebar_label: configuration
title: common.storages.configuration
---

## SchemaStorageConfiguration Objects

```python
@configspec
class SchemaStorageConfiguration(BaseConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/configuration.py#L25)

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

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/configuration.py#L45)

### normalize\_volume\_path

path to volume where normalized loader files will be stored

## FilesystemConfiguration Objects

```python
@configspec
class FilesystemConfiguration(BaseConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/configuration.py#L75)

A configuration defining filesystem location and access credentials.

When configuration is resolved, `bucket_url` is used to extract a protocol and request corresponding credentials class.
* s3
* gs, gcs
* az, abfs, adl
* file, memory
* gdrive

### read\_only

Indicates read only filesystem access. Will enable caching

### protocol

```python
@property
def protocol() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/configuration.py#L107)

`bucket_url` protocol

### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/configuration.py#L133)

Returns a fingerprint of bucket_url

### \_\_str\_\_

```python
def __str__() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/configuration.py#L137)

Return displayable destination location

