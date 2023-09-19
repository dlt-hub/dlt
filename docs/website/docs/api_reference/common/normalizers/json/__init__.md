---
sidebar_label: json
title: common.normalizers.json
---

## SupportsDataItemNormalizer Objects

```python
class SupportsDataItemNormalizer(Protocol)
```

Expected of modules defining data item normalizer

#### DataItemNormalizer

A class with a name DataItemNormalizer deriving from normalizers.json.DataItemNormalizer

#### wrap\_in\_dict

```python
def wrap_in_dict(item: Any) -> DictStrAny
```

Wraps `item` that is not a dictionary into dictionary that can be json normalized

