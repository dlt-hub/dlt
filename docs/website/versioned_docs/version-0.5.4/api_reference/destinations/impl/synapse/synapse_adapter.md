---
sidebar_label: synapse_adapter
title: destinations.impl.synapse.synapse_adapter
---

## TTableIndexType

Table [index type](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-tables-index) used when creating the Synapse table.
This regards indexes specified at the table level, not the column level.

## synapse\_adapter

```python
def synapse_adapter(data: Any,
                    table_index_type: TTableIndexType = None) -> DltResource
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/synapse/synapse_adapter.py#L18)

Prepares data for the Synapse destination by specifying which table index
type should be used.

**Arguments**:

- `data` _Any_ - The data to be transformed. It can be raw data or an instance
  of DltResource. If raw data, the function wraps it into a DltResource
  object.
- `table_index_type` _TTableIndexType, optional_ - The table index type used when creating
  the Synapse table.
  

**Returns**:

- `DltResource` - A resource with applied Synapse-specific hints.
  

**Raises**:

- `ValueError` - If input for `table_index_type` is invalid.
  

**Examples**:

```py
    data = [{"name": "Anush", "description": "Integrations Hacker"}]
    synapse_adapter(data, table_index_type="clustered_columnstore_index")
```
  [DltResource with hints applied]

