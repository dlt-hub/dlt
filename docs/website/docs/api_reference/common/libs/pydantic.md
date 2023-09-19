---
sidebar_label: pydantic
title: common.libs.pydantic
---

#### pydantic\_to\_table\_schema\_columns

```python
def pydantic_to_table_schema_columns(
        model: Union[BaseModel, Type[BaseModel]],
        skip_complex_types: bool = False) -> TTableSchemaColumns
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/libs/pydantic.py#L14)

Convert a pydantic model to a table schema columns dict

**Arguments**:

- `model` - The pydantic model to convert. Can be a class or an instance.
- `skip_complex_types` - If True, columns of complex types (`dict`, `list`, `BaseModel`) will be excluded from the result.
  

**Returns**:

- `TTableSchemaColumns` - table schema columns dict

