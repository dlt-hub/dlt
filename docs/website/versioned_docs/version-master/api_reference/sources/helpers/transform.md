---
sidebar_label: transform
title: sources.helpers.transform
---

## take\_first

```python
def take_first(max_items: int) -> ItemTransformFunctionNoMeta[bool]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/helpers/transform.py#L10)

A filter that takes only first `max_items` from a resource

## skip\_first

```python
def skip_first(max_items: int) -> ItemTransformFunctionNoMeta[bool]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/helpers/transform.py#L22)

A filter that skips first `max_items` from a resource

## pivot

```python
def pivot(paths: Union[str, Sequence[str]] = "$",
          prefix: str = "col") -> ItemTransformFunctionNoMeta[TDataItem]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/helpers/transform.py#L34)

Pivot the given sequence of sequences into a sequence of dicts,
generating column names from the given prefix and indexes, e.g.:
{"field": [[1, 2]]} -> {"field": [{"prefix_0": 1, "prefix_1": 2}]}

**Arguments**:

- `paths` _Union[str, Sequence[str]]_ - JSON paths of the fields to pivot.
- `prefix` _Optional[str]_ - Prefix to add to the column names.
  

**Returns**:

- `ItemTransformFunctionNoMeta[TDataItem]` - The transformer function.

## add\_row\_hash\_to\_table

```python
def add_row_hash_to_table(row_hash_column_name: str) -> TDataItem
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/helpers/transform.py#L115)

Computes content hash for each row of panda frame, arrow table or batch and adds it as `row_hash_column_name` column.

Internally arrow tables and batches are converted to pandas DataFrame and then `hash_pandas_object` is used to
generate a series with row hashes. Hashes are converted to signed int64 and added to original table. Data may be modified.
For SCD2 use with a resource configuration that assigns custom row version column to `row_hash_column_name`

