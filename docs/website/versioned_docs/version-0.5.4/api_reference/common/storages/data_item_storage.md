---
sidebar_label: data_item_storage
title: common.storages.data_item_storage
---

## DataItemStorage Objects

```python
class DataItemStorage(ABC)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/data_item_storage.py#L12)

### write\_empty\_items\_file

```python
def write_empty_items_file(load_id: str, schema_name: str, table_name: str,
                           columns: TTableSchemaColumns) -> DataWriterMetrics
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/data_item_storage.py#L43)

Writes empty file: only header and footer without actual items. Closed the
empty file and returns metrics. Mind that header and footer will be written.

### import\_items\_file

```python
def import_items_file(load_id: str, schema_name: str, table_name: str,
                      file_path: str,
                      metrics: DataWriterMetrics) -> DataWriterMetrics
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/data_item_storage.py#L51)

Import a file from `file_path` into items storage under a new file name. Does not check
the imported file format. Uses counts from `metrics` as a base. Logically closes the imported file

The preferred import method is a hard link to avoid copying the data. If current filesystem does not
support it, a regular copy is used.

### closed\_files

```python
def closed_files(load_id: str) -> List[DataWriterMetrics]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/data_item_storage.py#L78)

Return metrics for all fully processed (closed) files

### remove\_closed\_files

```python
def remove_closed_files(load_id: str) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/data_item_storage.py#L87)

Remove metrics for closed files in a given `load_id`

