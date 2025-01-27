---
sidebar_label: relational
title: common.normalizers.json.relational
---

## DataItemNormalizer Objects

```python
class DataItemNormalizer(DataItemNormalizerBase[RelationalNormalizerConfig])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/normalizers/json/relational.py#L45)

### C\_DLT\_ID

unique id of current row

### C\_DLT\_LOAD\_ID

load id to identify records loaded together that ie. need to be processed

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

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/normalizers/json/relational.py#L68)

This item normalizer works with nested dictionaries. It flattens dictionaries and descends into lists.
It yields row dictionaries at each nesting level.

### get\_row\_hash

```python
@staticmethod
def get_row_hash(row: Dict[str, Any],
                 subset: Optional[List[str]] = None) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/normalizers/json/relational.py#L168)

Returns hash of row.

Hash includes column names and values and is ordered by column name.
Excludes dlt system columns.
Can be used as deterministic row identifier.

### extend\_schema

```python
def extend_schema() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/normalizers/json/relational.py#L333)

Extends Schema with normalizer-specific hints and settings.

This method is called by Schema when instance is created or restored from storage.

### extend\_table

```python
def extend_table(table_name: str) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/normalizers/json/relational.py#L364)

If the table has a merge write disposition, add propagation info to normalizer

Called by Schema when new table is added to schema or table is updated with partial table.
Table name should be normalized.

