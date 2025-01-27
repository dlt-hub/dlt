---
sidebar_label: storage
title: extract.storage
---

## ExtractorItemStorage Objects

```python
class ExtractorItemStorage(DataItemStorage)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/storage.py#L20)

### \_\_init\_\_

```python
def __init__(package_storage: PackageStorage,
             writer_spec: FileWriterSpec) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/storage.py#L21)

Data item storage using `storage` to manage load packages

## ExtractStorage Objects

```python
class ExtractStorage(NormalizeStorage)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/storage.py#L34)

Wrapper around multiple extractor storages with different file formats

### create\_load\_package

```python
def create_load_package(schema: Schema,
                        reuse_exiting_package: bool = True) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/storage.py#L55)

Creates a new load package for given `schema` or returns if such package already exists.

You can prevent reuse of the existing package by setting `reuse_exiting_package` to False

### delete\_empty\_extract\_folder

```python
def delete_empty_extract_folder() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/storage.py#L99)

Deletes temporary extract folder if empty

### get\_load\_package\_info

```python
def get_load_package_info(load_id: str) -> LoadPackageInfo
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/storage.py#L103)

Returns information on temp and extracted packages

