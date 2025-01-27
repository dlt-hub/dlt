---
sidebar_label: sql_cs_v1
title: common.normalizers.naming.sql_cs_v1
---

## NamingConvention Objects

```python
class NamingConvention(BaseNamingConvention)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/normalizers/naming/sql_cs_v1.py#L14)

Generates case sensitive SQL safe identifiers, preserving the source casing.

- Spaces around identifier are trimmed
- Removes all ascii characters except ascii alphanumerics and underscores
- Prepends `_` if name starts with number.
- Removes all trailing underscores.
- Multiples of `_` are converted into single `_`.

