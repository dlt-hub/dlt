---
sidebar_label: duck_case
title: common.normalizers.naming.duck_case
---

## NamingConvention Objects

```python
class NamingConvention(SnakeCaseNamingConvention)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/normalizers/naming/duck_case.py#L7)

Case sensitive naming convention preserving all unicode characters except new line(s). Uses __ for path
separation and will replace multiple underscores with a single one.

