---
sidebar_label: transform
title: extract.incremental.transform
---

## IncrementalTransform Objects

```python
class IncrementalTransform()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/incremental/transform.py#L37)

A base class for handling extraction and stateful tracking
of incremental data from input data items.

By default, the descendant classes are instantiated within the
`dlt.extract.incremental.Incremental` class.

Subclasses must implement the `__call__` method which will be called
for each data item in the extracted data.

### deduplication\_disabled

```python
@property
def deduplication_disabled() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/incremental/transform.py#L108)

Skip deduplication when length of the key is 0

## JsonIncremental Objects

```python
class JsonIncremental(IncrementalTransform)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/incremental/transform.py#L113)

Extracts incremental data from JSON data items.

### find\_cursor\_value

```python
def find_cursor_value(row: TDataItem) -> Any
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/incremental/transform.py#L116)

Finds value in row at cursor defined by self.cursor_path.

Will use compiled JSONPath if present, otherwise it reverts to column search if row is dict

### \_\_call\_\_

```python
def __call__(row: TDataItem) -> Tuple[Optional[TDataItem], bool, bool]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/incremental/transform.py#L135)

**Returns**:

  Tuple (row, start_out_of_range, end_out_of_range) where row is either the data item or `None` if it is completely filtered out

