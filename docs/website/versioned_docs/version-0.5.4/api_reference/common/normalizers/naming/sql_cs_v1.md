---
sidebar_label: sql_cs_v1
title: common.normalizers.naming.sql_cs_v1
---

## NamingConvention Objects

```python
class NamingConvention(BaseNamingConvention)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/normalizers/naming/sql_cs_v1.py#L14)

Generates case sensitive SQL safe identifiers, preserving the source casing.

- Spaces around identifier are trimmed
- Removes all ascii characters except ascii alphanumerics and underscores
- Prepends `_` if name starts with number.
- Removes all trailing underscores.
- Multiples of `_` are converted into single `_`.

