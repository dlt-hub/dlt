---
sidebar_label: json
title: common.json
---

## SupportsJson Objects

```python
class SupportsJson(Protocol)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/json/__init__.py#L22)

Minimum adapter for different json parser implementations

## custom\_pua\_remove

```python
def custom_pua_remove(obj: Any) -> Any
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/json/__init__.py#L178)

Removes the PUA data type marker and leaves the correctly serialized type representation. Unmarked values are returned as-is.

## may\_have\_pua

```python
def may_have_pua(line: bytes) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/json/__init__.py#L188)

Checks if bytes string contains pua marker

