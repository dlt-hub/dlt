---
sidebar_label: direct
title: common.normalizers.naming.direct
---

## NamingConvention Objects

```python
class NamingConvention(BaseNamingConvention)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/normalizers/naming/direct.py#L6)

Case sensitive naming convention that maps source identifiers to destination identifiers with
only minimal changes. New line characters, double and single quotes are replaced with underscores.

Uses ▶ as path separator.

