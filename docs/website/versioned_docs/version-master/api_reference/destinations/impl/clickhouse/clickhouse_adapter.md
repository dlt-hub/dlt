---
sidebar_label: clickhouse_adapter
title: destinations.impl.clickhouse.clickhouse_adapter
---

## clickhouse\_adapter

```python
def clickhouse_adapter(data: Any,
                       table_engine_type: TTableEngineType = None
                       ) -> DltResource
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/clickhouse/clickhouse_adapter.py#L27)

Prepares data for the ClickHouse destination by specifying which table engine type
that should be used.

**Arguments**:

- `data` _Any_ - The data to be transformed. It can be raw data or an instance
  of DltResource. If raw data, the function wraps it into a DltResource
  object.
- `table_engine_type` _TTableEngineType, optional_ - The table index type used when creating
  the Synapse table.
  

**Returns**:

- `DltResource` - A resource with applied Synapse-specific hints.
  

**Raises**:

- `ValueError` - If input for `table_engine_type` is invalid.
  

**Examples**:

```py
    data = [{"name": "Alice", "description": "Software Developer"}]
    clickhouse_adapter(data, table_engine_type="merge_tree")
```
  [DltResource with hints applied]

