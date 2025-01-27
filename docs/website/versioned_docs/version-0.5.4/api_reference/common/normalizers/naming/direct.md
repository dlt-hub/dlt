---
sidebar_label: direct
title: common.normalizers.naming.direct
---

## NamingConvention Objects

```python
class NamingConvention(BaseNamingConvention)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/normalizers/naming/direct.py#L6)

Case sensitive naming convention that maps source identifiers to destination identifiers with
only minimal changes. New line characters, double and single quotes are replaced with underscores.

Uses ▶ as path separator.

