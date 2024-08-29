---
sidebar_label: exceptions
title: common.schema.exceptions
---

## DataValidationError Objects

```python
class DataValidationError(SchemaException)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/exceptions.py#L105)

### \_\_init\_\_

```python
def __init__(schema_name: str,
             table_name: str,
             column_name: str,
             schema_entity: TSchemaContractEntities,
             contract_mode: TSchemaEvolutionMode,
             table_schema: Any,
             schema_contract: TSchemaContractDict,
             data_item: Any = None,
             extended_info: str = None) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/schema/exceptions.py#L106)

Raised when `data_item` violates `contract_mode` on a `schema_entity` as defined by `table_schema`

Schema, table and column names are given as a context and full `schema_contract` and causing `data_item` as an evidence.

