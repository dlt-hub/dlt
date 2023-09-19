---
sidebar_label: configuration
title: common.storages.configuration
---

## SchemaStorageConfiguration Objects

```python
@configspec
class SchemaStorageConfiguration(BaseConfiguration)
```

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

#### normalize\_volume\_path

path to volume where normalized loader files will be stored

## LoadStorageConfiguration Objects

```python
@configspec
class LoadStorageConfiguration(BaseConfiguration)
```

#### load\_volume\_path

path to volume where files to be loaded to analytical storage are stored

#### delete\_completed\_jobs

if set to true the folder with completed jobs will be deleted

## FilesystemConfiguration Objects

```python
@configspec
class FilesystemConfiguration(BaseConfiguration)
```

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

`bucket_url` protocol

#### fingerprint

```python
def fingerprint() -> str
```

Returns a fingerprint of bucket_url

#### \_\_str\_\_

```python
def __str__() -> str
```

Return displayable destination location

