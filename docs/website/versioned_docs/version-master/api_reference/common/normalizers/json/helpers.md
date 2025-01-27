---
sidebar_label: helpers
title: common.normalizers.json.helpers
---

Cached helper methods for all operations that are called often

## get\_table\_nesting\_level

```python
@lru_cache(maxsize=None)
def get_table_nesting_level(schema: Schema,
                            table_name: str,
                            default_nesting: int = 1000) -> Optional[int]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/normalizers/json/helpers.py#L44)

gets table nesting level, will inherit from parent if not set

## is\_nested\_type

```python
@lru_cache(maxsize=None)
def is_nested_type(schema: Schema, table_name: str, field_name: str,
                   _r_lvl: int) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/normalizers/json/helpers.py#L67)

For those paths the nested objects should be left in place.
Cache perf: max_nesting < _r_lvl: ~2x faster, full check 10x faster

## get\_nested\_row\_id\_type

```python
@lru_cache(maxsize=None)
def get_nested_row_id_type(schema: Schema,
                           table_name: str) -> Tuple[TRowIdType, bool]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/normalizers/json/helpers.py#L95)

Gets type of row id to be added to nested table and if linking information should be added

## get\_row\_hash

```python
def get_row_hash(row: Dict[str, Any],
                 subset: Optional[List[str]] = None) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/normalizers/json/helpers.py#L124)

Returns hash of row.

Hash includes column names and values and is ordered by column name.
Excludes dlt system columns.
Can be used as deterministic row identifier.

