---
sidebar_label: data_item_storage
title: common.storages.data_item_storage
---

## DataItemStorage Objects

```python
class DataItemStorage(ABC)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/storages/data_item_storage.py#L15)

### write\_empty\_items\_file

```python
def write_empty_items_file(load_id: str, schema_name: str, table_name: str,
                           columns: TTableSchemaColumns) -> DataWriterMetrics
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/storages/data_item_storage.py#L47)

Writes empty file: only header and footer without actual items. Closed the
empty file and returns metrics. Mind that header and footer will be written.

### import\_items\_file

```python
def import_items_file(load_id: str,
                      schema_name: str,
                      table_name: str,
                      file_path: str,
                      metrics: DataWriterMetrics,
                      with_extension: str = None) -> DataWriterMetrics
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/storages/data_item_storage.py#L55)

Import a file from `file_path` into items storage under a new file name. Does not check
the imported file format. Uses counts from `metrics` as a base. Logically closes the imported file

The preferred import method is a hard link to avoid copying the data. If current filesystem does not
support it, a regular copy is used.

Alternative extension may be provided via `with_extension` so various file formats may be imported into the same folder.

### close\_writers

```python
def close_writers(load_id: str, skip_flush: bool = False) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/storages/data_item_storage.py#L75)

Flush, write footers (skip_flush), write metrics and close files in all
writers belonging to `load_id` package

### closed\_files

```python
def closed_files(load_id: str) -> List[DataWriterMetrics]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/storages/data_item_storage.py#L87)

Return metrics for all fully processed (closed) files

### remove\_closed\_files

```python
def remove_closed_files(load_id: str) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/storages/data_item_storage.py#L96)

Remove metrics for closed files in a given `load_id`

