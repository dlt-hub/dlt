---
sidebar_label: json
title: common.normalizers.json
---

## SupportsDataItemNormalizer Objects

```python
class SupportsDataItemNormalizer(Protocol)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/normalizers/json/__init__.py#L50)

Expected of modules defining data item normalizer

### DataItemNormalizer

A class with a name DataItemNormalizer deriving from normalizers.json.DataItemNormalizer

## wrap\_in\_dict

```python
def wrap_in_dict(item: Any) -> DictStrAny
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/normalizers/json/__init__.py#L57)

Wraps `item` that is not a dictionary into dictionary that can be json normalized

