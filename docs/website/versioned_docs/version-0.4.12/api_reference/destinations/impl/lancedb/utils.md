---
sidebar_label: utils
title: destinations.impl.lancedb.utils
---

## generate\_uuid

```python
def generate_uuid(data: DictStrAny, unique_identifiers: Sequence[str],
                  table_name: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/lancedb/utils.py#L19)

Generates deterministic UUID - used for deduplication.

**Arguments**:

- `data` _Dict[str, Any]_ - Arbitrary data to generate UUID for.
- `unique_identifiers` _Sequence[str]_ - A list of unique identifiers.
- `table_name` _str_ - LanceDB table name.
  

**Returns**:

- `str` - A string representation of the generated UUID.

## list\_merge\_identifiers

```python
def list_merge_identifiers(table_schema: TTableSchema) -> Sequence[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/lancedb/utils.py#L34)

Returns a list of merge keys for a table used for either merging or deduplication.

**Arguments**:

- `table_schema` _TTableSchema_ - a dlt table schema.
  

**Returns**:

- `Sequence[str]` - A list of unique column identifiers.

