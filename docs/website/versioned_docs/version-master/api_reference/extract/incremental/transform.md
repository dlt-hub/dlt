---
sidebar_label: transform
title: extract.incremental.transform
---

## IncrementalTransform Objects

```python
class IncrementalTransform()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/incremental/transform.py#L37)

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

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/incremental/transform.py#L107)

Skip deduplication when length of the key is 0 or if lag is applied.

## JsonIncremental Objects

```python
class JsonIncremental(IncrementalTransform)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/incremental/transform.py#L119)

Extracts incremental data from JSON data items.

### find\_cursor\_value

```python
def find_cursor_value(row: TDataItem) -> Any
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/incremental/transform.py#L122)

Finds value in row at cursor defined by self.cursor_path.

Will use compiled JSONPath if present.
Otherwise, reverts to field access if row is dict, Pydantic model, or of other class.

### \_\_call\_\_

```python
def __call__(row: TDataItem) -> Tuple[Optional[TDataItem], bool, bool]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/incremental/transform.py#L159)

**Returns**:

  Tuple (row, start_out_of_range, end_out_of_range) where row is either the data item or `None` if it is completely filtered out

