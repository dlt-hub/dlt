---
sidebar_label: type_mapping
title: destinations.type_mapping
---

## TypeMapperImpl Objects

```python
class TypeMapperImpl(DataTypeMapper)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/type_mapping.py#L15)

### sct\_to\_unbound\_dbt

Data types without precision or scale specified (e.g. `"text": "varchar"` in postgres)

### sct\_to\_dbt

Data types that require a precision or scale (e.g. `"text": "varchar(%i)"` or `"decimal": "numeric(%i,%i)"` in postgres).
Values should have printf placeholders for precision (and scale if applicable)

