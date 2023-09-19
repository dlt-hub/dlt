---
sidebar_label: json
title: common.json
---

## SupportsJson Objects

```python
class SupportsJson(Protocol)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/json/__init__.py#L20)

Minimum adapter for different json parser implementations

#### custom\_pua\_remove

```python
def custom_pua_remove(obj: Any) -> Any
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/json/__init__.py#L168)

Removes the PUA data type marker and leaves the correctly serialized type representation. Unmarked values are returned as-is.

