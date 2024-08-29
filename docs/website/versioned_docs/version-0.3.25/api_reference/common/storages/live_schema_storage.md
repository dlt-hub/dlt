---
sidebar_label: live_schema_storage
title: common.storages.live_schema_storage
---

## LiveSchemaStorage Objects

```python
class LiveSchemaStorage(SchemaStorage)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/live_schema_storage.py#L10)

### is\_live\_schema\_committed

```python
def is_live_schema_committed(name: str) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/live_schema_storage.py#L59)

Checks if live schema is present in storage and have same hash

### update\_live\_schema

```python
def update_live_schema(schema: Schema, can_create_new: bool = True) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/live_schema_storage.py#L70)

Will update live schema content without writing to storage. Optionally allows to create a new live schema

