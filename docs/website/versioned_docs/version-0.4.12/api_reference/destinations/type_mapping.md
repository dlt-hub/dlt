---
sidebar_label: type_mapping
title: destinations.type_mapping
---

## TypeMapper Objects

```python
class TypeMapper()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/type_mapping.py#L8)

### sct\_to\_unbound\_dbt

Data types without precision or scale specified (e.g. `"text": "varchar"` in postgres)

### sct\_to\_dbt

Data types that require a precision or scale (e.g. `"text": "varchar(%i)"` or `"decimal": "numeric(%i,%i)"` in postgres).
Values should have printf placeholders for precision (and scale if applicable)

