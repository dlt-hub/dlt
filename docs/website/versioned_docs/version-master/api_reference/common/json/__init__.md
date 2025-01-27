---
sidebar_label: json
title: common.json
---

## custom\_pua\_remove

```python
def custom_pua_remove(obj: Any) -> Any
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/json/__init__.py#L155)

Removes the PUA data type marker and leaves the correctly serialized type representation. Unmarked values are returned as-is.

## may\_have\_pua

```python
def may_have_pua(line: bytes) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/json/__init__.py#L165)

Checks if bytes string contains pua marker

## SupportsJson Objects

```python
class SupportsJson(Protocol)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/json/__init__.py#L170)

Minimum adapter for different json parser implementations

