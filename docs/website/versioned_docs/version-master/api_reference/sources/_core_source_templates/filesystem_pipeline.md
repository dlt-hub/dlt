---
sidebar_label: filesystem_pipeline
title: sources._core_source_templates.filesystem_pipeline
---

## stream\_and\_merge\_csv

```python
def stream_and_merge_csv() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/_core_source_templates/filesystem_pipeline.py#L14)

Demonstrates how to scan folder with csv files, load them in chunk and merge on date column with the previous load

## read\_custom\_file\_type\_excel

```python
def read_custom_file_type_excel() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/_core_source_templates/filesystem_pipeline.py#L103)

Here we create an extract pipeline using filesystem resource and read_csv transformer

## copy\_files\_resource

```python
def copy_files_resource(local_folder: str) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/_core_source_templates/filesystem_pipeline.py#L129)

Demonstrates how to copy files locally by adding a step to filesystem resource and the to load the download listing to db

