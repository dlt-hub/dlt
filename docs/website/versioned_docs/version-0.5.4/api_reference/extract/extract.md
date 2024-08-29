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
        write_disposition: TWriteDisposition = None,
        columns: TAnySchemaColumns = None,
        primary_key: TColumnNames = None,
        schema_contract: TSchemaContract = None) -> List[DltSource]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/extract.py#L44)

Creates a list of sources for data items present in `data` and applies specified hints to all resources.

`data` may be a DltSource, DltResource, a list of those or any other data type accepted by pipeline.run

## describe\_extract\_data

```python
def describe_extract_data(data: Any) -> List[ExtractDataInfo]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/extract.py#L133)

Extract source and resource names from data passed to extract

## Extract Objects

```python
class Extract(WithStepInfo[ExtractMetrics, ExtractInfo])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/extract.py#L166)

### \_\_init\_\_

```python
def __init__(schema_storage: SchemaStorage,
             normalize_storage_config: NormalizeStorageConfiguration,
             collector: Collector = NULL_COLLECTOR,
             original_data: Any = None) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/extract.py#L167)

optionally saves originally extracted `original_data` to generate extract info

### commit\_packages

```python
def commit_packages() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/extract.py#L358)

Commits all extracted packages to normalize storage

