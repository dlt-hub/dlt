---
sidebar_label: extract
title: extract.extract
---

## data\_to\_sources

```python
def data_to_sources(
        data: Any,
        pipeline: SupportsPipeline,
        schema: Schema = None,
        table_name: str = None,
        parent_table_name: str = None,
        write_disposition: TWriteDispositionConfig = None,
        columns: TAnySchemaColumns = None,
        primary_key: TColumnNames = None,
        schema_contract: TSchemaContract = None) -> List[DltSource]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/extract.py#L50)

Creates a list of sources for data items present in `data` and applies specified hints to all resources.

`data` may be a DltSource, DltResource, a list of those or any other data type accepted by pipeline.run

## describe\_extract\_data

```python
def describe_extract_data(data: Any) -> List[ExtractDataInfo]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/extract.py#L138)

Extract source and resource names from data passed to extract

## Extract Objects

```python
class Extract(WithStepInfo[ExtractMetrics, ExtractInfo])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/extract.py#L171)

### original\_data

Original data from which the extracted DltSource was created. Will be used to describe in extract info

### \_\_init\_\_

```python
def __init__(schema_storage: SchemaStorage,
             normalize_storage_config: NormalizeStorageConfiguration,
             collector: Collector = NULL_COLLECTOR,
             original_data: Any = None) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/extract.py#L175)

optionally saves originally extracted `original_data` to generate extract info

### commit\_packages

```python
def commit_packages() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/extract.py#L413)

Commits all extracted packages to normalize storage

