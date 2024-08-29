---
sidebar_label: transform
title: extract.incremental.transform
---

## IncrementalTransform Objects

```python
class IncrementalTransform()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/incremental/transform.py#L32)

### deduplication\_disabled

```python
@property
def deduplication_disabled() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/incremental/transform.py#L93)

Skip deduplication when length of the key is 0

## JsonIncremental Objects

```python
class JsonIncremental(IncrementalTransform)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/incremental/transform.py#L98)

### find\_cursor\_value

```python
def find_cursor_value(row: TDataItem) -> Any
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/incremental/transform.py#L99)

Finds value in row at cursor defined by self.cursor_path.

Will use compiled JSONPath if present, otherwise it reverts to column search if row is dict

### \_\_call\_\_

```python
def __call__(row: TDataItem) -> Tuple[Optional[TDataItem], bool, bool]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/incremental/transform.py#L118)

**Returns**:

  Tuple (row, start_out_of_range, end_out_of_range) where row is either the data item or `None` if it is completely filtered out

