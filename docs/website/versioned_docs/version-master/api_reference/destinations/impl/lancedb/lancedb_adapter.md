---
sidebar_label: lancedb_adapter
title: destinations.impl.lancedb.lancedb_adapter
---

## lancedb\_adapter

```python
def lancedb_adapter(data: Any,
                    embed: TColumnNames = None,
                    merge_key: TColumnNames = None,
                    no_remove_orphans: bool = False) -> DltResource
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/lancedb/lancedb_adapter.py#L14)

Prepares data for the LanceDB destination by specifying which columns should be embedded.

**Arguments**:

- `data` _Any_ - The data to be transformed. It can be raw data or an instance
  of DltResource. If raw data, the function wraps it into a DltResource
  object.
- `embed` _TColumnNames, optional_ - Specify columns to generate embeddings for.
  It can be a single column name as a string, or a list of column names.
- `merge_key` _TColumnNames, optional_ - Specify columns to merge on.
  It can be a single column name as a string, or a list of column names.
- `no_remove_orphans` _bool_ - Specify whether to remove orphaned records in child
  tables with no parent records after merges to maintain referential integrity.
  

**Returns**:

- `DltResource` - A resource with applied LanceDB-specific hints.
  

**Raises**:

- `ValueError` - If input for `embed` invalid or empty.
  

**Examples**:

```py
    data = [{"name": "Marcel", "description": "Moonbase Engineer"}]
    lancedb_adapter(data, embed="description")
```
  [DltResource with hints applied]

