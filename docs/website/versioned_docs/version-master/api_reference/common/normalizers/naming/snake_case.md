---
sidebar_label: snake_case
title: common.normalizers.naming.snake_case
---

## NamingConvention Objects

```python
class NamingConvention(BaseNamingConvention)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/normalizers/naming/snake_case.py#L14)

Case insensitive naming convention, converting source identifiers into lower case snake case with reduced alphabet.

- Spaces around identifier are trimmed
- Removes all ascii characters except ascii alphanumerics and underscores
- Prepends `_` if name starts with number.
- Multiples of `_` are converted into single `_`.
- Replaces all trailing `_` with `x`
- Replaces `+` and `*` with `x`, `-` with `_`, `@` with `a` and `|` with `l`

Uses __ as parent-child separator for tables and flattened column names.

