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
                            type_mapper: TypeMapper) -> TArrowField
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/lancedb/schema.py#L30)

Creates a PyArrow field from a dlt column schema.

## make\_arrow\_table\_schema

```python
def make_arrow_table_schema(
        table_name: str,
        schema: Schema,
        type_mapper: TypeMapper,
        id_field_name: Optional[str] = None,
        vector_field_name: Optional[str] = None,
        embedding_fields: Optional[List[str]] = None,
        embedding_model_func: Optional[TextEmbeddingFunction] = None,
        embedding_model_dimensions: Optional[int] = None) -> TArrowSchema
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/lancedb/schema.py#L40)

Creates a PyArrow schema from a dlt schema.

