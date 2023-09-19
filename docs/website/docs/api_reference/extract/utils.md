---
sidebar_label: utils
title: extract.utils
---

#### resolve\_column\_value

```python
def resolve_column_value(column_hint: TTableHintTemplate[TColumnNames],
                         item: TDataItem) -> Union[Any, List[Any]]
```

Extract values from the data item given a column hint.
Returns either a single value or list of values when hint is a composite.

#### ensure\_table\_schema\_columns

```python
def ensure_table_schema_columns(
        columns: TAnySchemaColumns) -> TTableSchemaColumns
```

Convert supported column schema types to a column dict which
can be used in resource schema.

**Arguments**:

- `columns` - A dict of column schemas, a list of column schemas, or a pydantic model

#### ensure\_table\_schema\_columns\_hint

```python
def ensure_table_schema_columns_hint(
    columns: TTableHintTemplate[TAnySchemaColumns]
) -> TTableHintTemplate[TTableSchemaColumns]
```

Convert column schema hint to a hint returning `TTableSchemaColumns`.
A callable hint is wrapped in another function which converts the original result.

