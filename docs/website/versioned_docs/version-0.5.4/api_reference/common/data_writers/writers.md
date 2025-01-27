---
sidebar_label: writers
title: common.data_writers.writers
---

## FileWriterSpec Objects

```python
class FileWriterSpec(NamedTuple)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/data_writers/writers.py#L50)

### file\_format

format of the output file

### data\_item\_format

format of the input data

### supports\_schema\_changes

File format supports changes of schema: True - at any moment, Buffer - in memory buffer before opening file,  False - not at all

## DataWriter Objects

```python
class DataWriter(abc.ABC)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/data_writers/writers.py#L66)

### item\_format\_from\_file\_extension

```python
@classmethod
def item_format_from_file_extension(cls, extension: str) -> TDataItemFormat
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/data_writers/writers.py#L111)

Simple heuristic to get data item format from file extension

## ImportFileWriter Objects

```python
class ImportFileWriter(DataWriter)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/data_writers/writers.py#L145)

May only import files, fails on any open/write operations

## ArrowToObjectAdapter Objects

```python
class ArrowToObjectAdapter()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/data_writers/writers.py#L633)

A mixin that will convert object writer into arrow writer.

## is\_native\_writer

```python
def is_native_writer(writer_type: Type[DataWriter]) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/data_writers/writers.py#L665)

Checks if writer has adapter mixin. Writers with adapters are not native and typically
decrease the performance.

## resolve\_best\_writer\_spec

```python
def resolve_best_writer_spec(
        item_format: TDataItemFormat,
        possible_file_formats: Sequence[TLoaderFileFormat],
        preferred_format: TLoaderFileFormat = None) -> FileWriterSpec
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/data_writers/writers.py#L706)

Finds best writer for `item_format` out of `possible_file_formats`. Tries `preferred_format` first.
Best possible writer is a native writer for `item_format` writing files in `preferred_format`.
If not found, any native writer for `possible_file_formats` is picked.
Native writer supports `item_format` directly without a need to convert to other item formats.

## get\_best\_writer\_spec

```python
def get_best_writer_spec(item_format: TDataItemFormat,
                         file_format: TLoaderFileFormat) -> FileWriterSpec
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/data_writers/writers.py#L760)

Gets writer for `item_format` writing files in {file_format}. Looks for native writer first

## create\_import\_spec

```python
def create_import_spec(
        item_file_format: TLoaderFileFormat,
        possible_file_formats: Sequence[TLoaderFileFormat]) -> FileWriterSpec
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/data_writers/writers.py#L771)

Creates writer spec that may be used only to import files

