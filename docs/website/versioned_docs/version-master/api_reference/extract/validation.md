---
sidebar_label: validation
title: extract.validation
---

## PydanticValidator Objects

```python
class PydanticValidator(ValidateItem, Generic[_TPydanticModel])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/validation.py#L17)

### \_\_call\_\_

```python
def __call__(item: TDataItems, meta: Any = None) -> TDataItems
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/validation.py#L33)

Validate a data item against the pydantic model

## create\_item\_validator

```python
def create_item_validator(
    columns: TTableHintTemplate[TAnySchemaColumns],
    schema_contract: TTableHintTemplate[TSchemaContract] = None
) -> Tuple[Optional[ValidateItem], TTableHintTemplate[TSchemaContract]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/validation.py#L58)

Creates item validator for a `columns` definition and a `schema_contract`

Returns a tuple (validator, schema contract). If validator could not be created, returns None at first position.
If schema_contract was not specified a default schema contract for given validator will be returned

