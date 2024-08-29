---
sidebar_label: utils
title: common.destination.utils
---

## verify\_schema\_capabilities

```python
def verify_schema_capabilities(schema: Schema,
                               capabilities: DestinationCapabilitiesContext,
                               destination_type: str,
                               warnings: bool = True) -> List[Exception]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/destination/utils.py#L14)

Verifies schema tables before loading against capabilities. Returns a list of exceptions representing critical problems with the schema.
It will log warnings by default. It is up to the caller to eventually raise exception

* Checks all table and column name lengths against destination capabilities and raises on too long identifiers
* Checks if schema has collisions due to case sensitivity of the identifiers

