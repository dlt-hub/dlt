---
sidebar_label: live_schema_storage
title: common.storages.live_schema_storage
---

## LiveSchemaStorage Objects

```python
class LiveSchemaStorage(SchemaStorage)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/live_schema_storage.py#L9)

### commit\_live\_schema

```python
def commit_live_schema(name: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/live_schema_storage.py#L43)

Saves live schema in storage if it was modified

### is\_live\_schema\_committed

```python
def is_live_schema_committed(name: str) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/live_schema_storage.py#L51)

Checks if live schema is present in storage and have same hash

### set\_live\_schema

```python
def set_live_schema(schema: Schema) -> Schema
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/live_schema_storage.py#L58)

Will add or update live schema content without writing to storage.

