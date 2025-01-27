---
sidebar_label: relational
title: common.normalizers.json.relational
---

## DataItemNormalizer Objects

```python
class DataItemNormalizer(DataItemNormalizerBase[RelationalNormalizerConfig])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/normalizers/json/relational.py#L38)

### C\_DLT\_ROOT\_ID

unique id of top level parent

### C\_DLT\_PARENT\_ID

unique id of parent row

### C\_DLT\_LIST\_IDX

position in the list of rows

### C\_VALUE

for lists of simple types

### EMPTY\_KEY\_IDENTIFIER

replace empty keys with this

### \_\_init\_\_

```python
def __init__(schema: Schema) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/normalizers/json/relational.py#L57)

This item normalizer works with nested dictionaries. It flattens dictionaries and descends into lists.
It yields row dictionaries at each nesting level.

### extend\_schema

```python
def extend_schema() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/normalizers/json/relational.py#L274)

Extends Schema with normalizer-specific hints and settings.

This method is called by Schema when instance is created or restored from storage.

### extend\_table

```python
def extend_table(table_name: str) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/normalizers/json/relational.py#L306)

If the table has a merge write disposition, add propagation info to normalizer

Called by Schema when new table is added to schema or table is updated with partial table.
Table name should be normalized.

