---
sidebar_label: lancedb_adapter
title: destinations.impl.lancedb.lancedb_adapter
---

## lancedb\_adapter

```python
def lancedb_adapter(data: Any, embed: TColumnNames = None) -> DltResource
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/lancedb/lancedb_adapter.py#L11)

Prepares data for the LanceDB destination by specifying which columns should be embedded.

**Arguments**:

- `data` _Any_ - The data to be transformed. It can be raw data or an instance
  of DltResource. If raw data, the function wraps it into a DltResource
  object.
- `embed` _TColumnNames, optional_ - Specify columns to generate embeddings for.
  It can be a single column name as a string, or a list of column names.
  

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

