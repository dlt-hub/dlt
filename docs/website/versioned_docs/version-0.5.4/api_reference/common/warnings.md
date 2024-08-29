---
sidebar_label: warnings
title: common.warnings
---

## DltDeprecationWarning Objects

```python
class DltDeprecationWarning(DeprecationWarning)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/warnings.py#L12)

A dlt specific deprecation warning.

This warning is raised when using deprecated functionality in dlt. It provides information on when the
deprecation was introduced and the expected version in which the corresponding functionality will be removed.

**Attributes**:

- `message` - Description of the warning.
- `since` - Version in which the deprecation was introduced.
- `expected_due` - Version in which the corresponding functionality is expected to be removed.

