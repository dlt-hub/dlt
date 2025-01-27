---
sidebar_label: buffered
title: common.data_writers.buffered
---

## new\_file\_id

```python
def new_file_id() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/data_writers/buffered.py#L22)

Creates new file id which is globally unique within table_name scope

## BufferedDataWriter Objects

```python
class BufferedDataWriter(Generic[TWriter])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/data_writers/buffered.py#L27)

### write\_empty\_file

```python
def write_empty_file(columns: TTableSchemaColumns) -> DataWriterMetrics
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/data_writers/buffered.py#L123)

Writes empty file: only header and footer without actual items. Closed the
empty file and returns metrics. Mind that header and footer will be written.

### import\_file

```python
def import_file(file_path: str,
                metrics: DataWriterMetrics,
                with_extension: str = None) -> DataWriterMetrics
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/data_writers/buffered.py#L132)

Import a file from `file_path` into items storage under a new file name. Does not check
the imported file format. Uses counts from `metrics` as a base. Logically closes the imported file

The preferred import method is a hard link to avoid copying the data. If current filesystem does not
support it, a regular copy is used.

Alternative extension may be provided via `with_extension` so various file formats may be imported into the same folder.

### close

```python
def close(skip_flush: bool = False) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/data_writers/buffered.py#L172)

Flushes the data, writes footer (skip_flush is True), collects metrics and closes the underlying file.

### alternative\_spec

```python
@contextlib.contextmanager
def alternative_spec(spec: FileWriterSpec) -> Iterator[FileWriterSpec]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/data_writers/buffered.py#L184)

Temporarily changes the writer spec ie. for the moment file is rotated

