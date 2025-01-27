---
sidebar_label: pydantic
title: common.libs.pydantic
---

## DltConfig Objects

```python
class DltConfig(TypedDict)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/libs/pydantic.py#L66)

dlt configuration that can be attached to Pydantic model

Example below removes `nested` field from the resulting dlt schema.
```py
class ItemModel(BaseModel):
    b: bool
    nested: Dict[str, Any]
    dlt_config: ClassVar[DltConfig] = {"skip_nested_types": True}
```

### skip\_nested\_types

If True, columns of complex types (`dict`, `list`, `BaseModel`) will be excluded from dlt schema generated from the model

### skip\_complex\_types

deprecated

## pydantic\_to\_table\_schema\_columns

```python
def pydantic_to_table_schema_columns(
        model: Union[BaseModel, Type[BaseModel]]) -> TTableSchemaColumns
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/libs/pydantic.py#L81)

Convert a pydantic model to a table schema columns dict

See also DltConfig for more control over how the schema is created

**Arguments**:

- `model` - The pydantic model to convert. Can be a class or an instance.
  
  

**Returns**:

- `TTableSchemaColumns` - table schema columns dict

## apply\_schema\_contract\_to\_model

```python
def apply_schema_contract_to_model(
        model: Type[_TPydanticModel],
        column_mode: TSchemaEvolutionMode,
        data_mode: TSchemaEvolutionMode = "freeze") -> Type[_TPydanticModel]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/libs/pydantic.py#L205)

Configures or re-creates `model` so it behaves according to `column_mode` and `data_mode` settings.

`column_mode` sets the model behavior when unknown field is found.
`data_mode` sets model behavior when known field does not validate. currently `evolve` and `freeze` are supported here.

`discard_row` is implemented in `validate_item`.

## create\_list\_model

```python
def create_list_model(
    model: Type[_TPydanticModel],
    data_mode: TSchemaEvolutionMode = "freeze"
) -> Type[ListModel[_TPydanticModel]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/libs/pydantic.py#L297)

Creates a model from `model` for validating list of items in batch according to `data_mode`

Currently only freeze is supported. See comments in the code

## validate\_and\_filter\_items

```python
def validate_and_filter_items(
        table_name: str, list_model: Type[ListModel[_TPydanticModel]],
        items: List[TDataItem], column_mode: TSchemaEvolutionMode,
        data_mode: TSchemaEvolutionMode) -> List[_TPydanticModel]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/libs/pydantic.py#L312)

Validates list of `item` with `list_model` and returns parsed Pydantic models. If `column_mode` and `data_mode` are set
this function will remove non validating items (`discard_row`) or raise on the first non-validating items (`freeze`). Note
that the model itself may be configured to remove non validating or extra items as well.

`list_model` should be created with `create_list_model` and have `items` field which this function returns.

## validate\_and\_filter\_item

```python
def validate_and_filter_item(
        table_name: str, model: Type[_TPydanticModel], item: TDataItems,
        column_mode: TSchemaEvolutionMode,
        data_mode: TSchemaEvolutionMode) -> Optional[_TPydanticModel]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/libs/pydantic.py#L398)

Validates `item` against model `model` and returns an instance of it. If `column_mode` and `data_mode` are set
this function will return None (`discard_row`) or raise on non-validating items (`freeze`). Note
that the model itself may be configured to remove non validating or extra items as well.

