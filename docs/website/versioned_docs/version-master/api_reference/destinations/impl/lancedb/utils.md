---
sidebar_label: utils
title: destinations.impl.lancedb.utils
---

## fill\_empty\_source\_column\_values\_with\_placeholder

```python
def fill_empty_source_column_values_with_placeholder(
        table: pa.Table, source_columns: List[str],
        placeholder: str) -> pa.Table
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/lancedb/utils.py#L47)

Replaces empty strings and null values in the specified source columns of an Arrow table with a placeholder string.

**Arguments**:

- `table` _pa.Table_ - The input Arrow table.
- `source_columns` _List[str]_ - A list of column names to replace empty strings and null values in.
- `placeholder` _str_ - The placeholder string to use for replacement.
  

**Returns**:

- `pa.Table` - The modified Arrow table with empty strings and null values replaced in the specified columns.

