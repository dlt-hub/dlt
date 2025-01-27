---
sidebar_label: postgres_adapter
title: destinations.impl.postgres.postgres_adapter
---

## postgres\_adapter

```python
def postgres_adapter(data: Any,
                     geometry: TColumnNames = None,
                     srid: Optional[int] = 4326) -> DltResource
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/postgres/postgres_adapter.py#L11)

Prepares data for the postgres destination by specifying which columns should
be cast to PostGIS geometry types.

**Arguments**:

- `data` _Any_ - The data to be transformed. It can be raw data or an instance
  of DltResource. If raw data, the function wraps it into a DltResource
  object.
- `geometry` _TColumnNames, optional_ - Specify columns to cast to geometries.
  It can be a single column name as a string, or a list of column names.
- `srid` _int, optional_ - The Spatial Reference System Identifier (SRID) to be
  used for the geometry columns. If not provided, SRID 4326 will be used.
  

**Returns**:

- `DltResource` - A resource with applied postgres-specific hints.
  

**Raises**:

- `ValueError` - If input for `geometry` is invalid, or if no geometry columns are specified.
  

**Examples**:

```py
    data = [{"town": "Null Island", "loc": "POINT(0 0)"}]
    postgres_adapter(data, geometry="loc", srid=4326)
```
  [DltResource with hints applied]

