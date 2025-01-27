---
sidebar_label: type_mapping
title: destinations.type_mapping
---

## TypeMapper Objects

```python
class TypeMapper()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/type_mapping.py#L8)

### sct\_to\_unbound\_dbt

Data types without precision or scale specified (e.g. `"text": "varchar"` in postgres)

### sct\_to\_dbt

Data types that require a precision or scale (e.g. `"text": "varchar(%i)"` or `"decimal": "numeric(%i,%i)"` in postgres).
Values should have printf placeholders for precision (and scale if applicable)

