---
sidebar_label: schema_types
title: sources.sql_database.schema_types
---

## default\_table\_adapter

```python
def default_table_adapter(table: Table,
                          included_columns: Optional[List[str]]) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/sql_database/schema_types.py#L47)

Default table adapter being always called before custom one

## sqla\_col\_to\_column\_schema

```python
def sqla_col_to_column_schema(
        sql_col: ColumnAny,
        reflection_level: ReflectionLevel,
        type_adapter_callback: Optional[TTypeAdapter] = None,
        skip_nested_columns_on_minimal: bool = False
) -> Optional[TColumnSchema]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/sql_database/schema_types.py#L61)

Infer dlt schema column type from an sqlalchemy type.

If `add_precision` is set, precision and scale is inferred from that types that support it,
such as numeric, varchar, int, bigint. Numeric (decimal) types have always precision added.

## get\_primary\_key

```python
def get_primary_key(table: Table) -> Optional[List[str]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/sql_database/schema_types.py#L154)

Create primary key or return None if no key defined

## table\_to\_columns

```python
def table_to_columns(
        table: Table,
        reflection_level: ReflectionLevel = "full",
        type_conversion_fallback: Optional[TTypeAdapter] = None,
        skip_nested_columns_on_minimal: bool = False) -> TTableSchemaColumns
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/sql_database/schema_types.py#L160)

Convert an sqlalchemy table to a dlt table schema.

## get\_table\_references

```python
def get_table_references(table: Table) -> Optional[List[TTableReference]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/sql_database/schema_types.py#L179)

Resolve table references from SQLAlchemy foreign key constraints in the table

