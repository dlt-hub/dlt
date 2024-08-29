---
sidebar_label: json
title: common.json
---

## SupportsJson Objects

```python
class SupportsJson(Protocol)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/json/__init__.py#L21)

Minimum adapter for different json parser implementations

## custom\_pua\_remove

```python
def custom_pua_remove(obj: Any) -> Any
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/json/__init__.py#L177)

Removes the PUA data type marker and leaves the correctly serialized type representation. Unmarked values are returned as-is.

## may\_have\_pua

```python
def may_have_pua(line: bytes) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/json/__init__.py#L187)

Checks if bytes string contains pua marker

