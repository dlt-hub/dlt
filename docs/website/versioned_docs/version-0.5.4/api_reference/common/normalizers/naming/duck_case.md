---
sidebar_label: duck_case
title: common.normalizers.naming.duck_case
---

## NamingConvention Objects

```python
class NamingConvention(SnakeCaseNamingConvention)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/normalizers/naming/duck_case.py#L7)

Case sensitive naming convention preserving all unicode characters except new line(s). Uses __ for path
separation and will replace multiple underscores with a single one.

