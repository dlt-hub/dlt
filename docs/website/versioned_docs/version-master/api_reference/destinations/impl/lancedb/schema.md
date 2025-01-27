---
sidebar_label: schema
title: destinations.impl.lancedb.schema
---

Utilities for creating arrow schemas from table schemas.

## NULL\_SCHEMA

Empty pyarrow Schema with no fields.

## make\_arrow\_field\_schema

```python
def make_arrow_field_schema(column_name: str, column: TColumnSchema,
                            type_mapper: DataTypeMapper) -> TArrowField
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/lancedb/schema.py#L32)

Creates a PyArrow field from a dlt column schema.

## make\_arrow\_table\_schema

```python
def make_arrow_table_schema(
        table_name: str,
        schema: Schema,
        type_mapper: DataTypeMapper,
        vector_field_name: Optional[str] = None,
        embedding_fields: Optional[List[str]] = None,
        embedding_model_func: Optional[TextEmbeddingFunction] = None,
        embedding_model_dimensions: Optional[int] = None) -> TArrowSchema
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/lancedb/schema.py#L42)

Creates a PyArrow schema from a dlt schema.

