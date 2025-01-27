---
sidebar_label: qdrant_adapter
title: destinations.impl.qdrant.qdrant_adapter
---

## qdrant\_adapter

```python
def qdrant_adapter(data: Any, embed: TColumnNames = None) -> DltResource
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/qdrant/qdrant_adapter.py#L11)

Prepares data for the Qdrant destination by specifying which columns
should be embedded.

**Arguments**:

- `data` _Any_ - The data to be transformed. It can be raw data or an instance
  of DltResource. If raw data, the function wraps it into a DltResource
  object.
- `embed` _TColumnNames, optional_ - Specifies columns to generate embeddings for.
  Can be a single column name as a string or a list of column names.
  

**Returns**:

- `DltResource` - A resource with applied qdrant-specific hints.
  

**Raises**:

- `ValueError` - If input for `embed` invalid or empty.
  

**Examples**:

```py
    data = [{"name": "Anush", "description": "Integrations Hacker"}]
    qdrant_adapter(data, embed="description")
```
  [DltResource with hints applied]

