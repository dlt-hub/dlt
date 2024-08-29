---
sidebar_label: relational
title: common.normalizers.json.relational
---

## EMPTY\_KEY\_IDENTIFIER

replace empty keys with this

## TDataItemRowChild Objects

```python
class TDataItemRowChild(TDataItemRow)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/normalizers/json/relational.py#L31)

### value

for lists of simple types

## DataItemNormalizer Objects

```python
class DataItemNormalizer(DataItemNormalizerBase[RelationalNormalizerConfig])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/normalizers/json/relational.py#L49)

### \_\_init\_\_

```python
def __init__(schema: Schema) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/normalizers/json/relational.py#L55)

This item normalizer works with nested dictionaries. It flattens dictionaries and descends into lists.
It yields row dictionaries at each nesting level.

