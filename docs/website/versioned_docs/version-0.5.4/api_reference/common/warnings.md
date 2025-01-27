---
sidebar_label: warnings
title: common.warnings
---

## DltDeprecationWarning Objects

```python
class DltDeprecationWarning(DeprecationWarning)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/warnings.py#L12)

A dlt specific deprecation warning.

This warning is raised when using deprecated functionality in dlt. It provides information on when the
deprecation was introduced and the expected version in which the corresponding functionality will be removed.

**Attributes**:

- `message` - Description of the warning.
- `since` - Version in which the deprecation was introduced.
- `expected_due` - Version in which the corresponding functionality is expected to be removed.

