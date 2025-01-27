---
sidebar_label: duck_case
title: common.normalizers.naming.duck_case
---

## NamingConvention Objects

```python
class NamingConvention(SnakeCaseNamingConvention)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/normalizers/naming/duck_case.py#L7)

Case sensitive naming convention preserving all unicode characters except new line(s). Uses __ for path
separation and will replace multiple underscores with a single one.

