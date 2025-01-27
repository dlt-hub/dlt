---
sidebar_label: exceptions
title: common.schema.exceptions
---

## DataValidationError Objects

```python
class DataValidationError(SchemaException)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/exceptions.py#L151)

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

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/exceptions.py#L152)

Raised when `data_item` violates `contract_mode` on a `schema_entity` as defined by `table_schema`

Schema, table and column names are given as a context and full `schema_contract` and causing `data_item` as an evidence.

