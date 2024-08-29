---
sidebar_label: type_mapping
title: destinations.type_mapping
---

## TypeMapper Objects

```python
class TypeMapper()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/type_mapping.py#L8)

### sct\_to\_unbound\_dbt

Data types without precision or scale specified (e.g. `"text": "varchar"` in postgres)

### sct\_to\_dbt

Data types that require a precision or scale (e.g. `"text": "varchar(%i)"` or `"decimal": "numeric(%i,%i)"` in postgres).
Values should have printf placeholders for precision (and scale if applicable)

