---
sidebar_label: configuration
title: common.storages.configuration
---

## SchemaStorageConfiguration Objects

```python
@configspec
class SchemaStorageConfiguration(BaseConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/storages/configuration.py#L15)

#### schema\_volume\_path

path to volume with default schemas

#### import\_schema\_path

the import schema from external location

#### export\_schema\_path

the export schema to external location

#### external\_schema\_format

format in which to expect external schema

#### external\_schema\_format\_remove\_defaults

remove default values when exporting schema

## NormalizeStorageConfiguration Objects

```python
@configspec
class NormalizeStorageConfiguration(BaseConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/storages/configuration.py#L28)

#### normalize\_volume\_path

path to volume where normalized loader files will be stored

## LoadStorageConfiguration Objects

```python
@configspec
class LoadStorageConfiguration(BaseConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/storages/configuration.py#L37)

#### load\_volume\_path

path to volume where files to be loaded to analytical storage are stored

#### delete\_completed\_jobs

if set to true the folder with completed jobs will be deleted

## FilesystemConfiguration Objects

```python
@configspec
class FilesystemConfiguration(BaseConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/storages/configuration.py#L49)

A configuration defining filesystem location and access credentials.

When configuration is resolved, `bucket_url` is used to extract a protocol and request corresponding credentials class.
* s3
* gs, gcs
* az, abfs, adl
* file, memory
* gdrive

#### protocol

```python
@property
def protocol() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/storages/configuration.py#L74)

`bucket_url` protocol

#### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/storages/configuration.py#L93)

Returns a fingerprint of bucket_url

#### \_\_str\_\_

```python
def __str__() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/storages/configuration.py#L99)

Return displayable destination location

