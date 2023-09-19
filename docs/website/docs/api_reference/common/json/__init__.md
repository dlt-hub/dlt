---
sidebar_label: json
title: common.json
---

## SupportsJson Objects

```python
class SupportsJson(Protocol)
```

Minimum adapter for different json parser implementations

#### custom\_pua\_remove

```python
def custom_pua_remove(obj: Any) -> Any
```

Removes the PUA data type marker and leaves the correctly serialized type representation. Unmarked values are returned as-is.

