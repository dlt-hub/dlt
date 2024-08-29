---
sidebar_label: validation
title: extract.validation
---

## PydanticValidator Objects

```python
class PydanticValidator(ValidateItem, Generic[_TPydanticModel])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/validation.py#L17)

### \_\_call\_\_

```python
def __call__(
        item: TDataItems,
        meta: Any = None) -> Union[_TPydanticModel, List[_TPydanticModel]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/validation.py#L33)

Validate a data item against the pydantic model

## create\_item\_validator

```python
def create_item_validator(
    columns: TTableHintTemplate[TAnySchemaColumns],
    schema_contract: TTableHintTemplate[TSchemaContract] = None
) -> Tuple[Optional[ValidateItem], TTableHintTemplate[TSchemaContract]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/validation.py#L52)

Creates item validator for a `columns` definition and a `schema_contract`

Returns a tuple (validator, schema contract). If validator could not be created, returns None at first position.
If schema_contract was not specified a default schema contract for given validator will be returned

