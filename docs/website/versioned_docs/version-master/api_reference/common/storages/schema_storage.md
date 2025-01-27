---
sidebar_label: schema_storage
title: common.storages.schema_storage
---

## SchemaStorage Objects

```python
class SchemaStorage(Mapping[str, Schema])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/schema_storage.py#L25)

### save\_schema

```python
def save_schema(schema: Schema) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/schema_storage.py#L59)

Saves schema to the storage and returns the path relative to storage.

If import schema path is configured and import schema with schema.name exits, it
will be linked to `schema` via `_imported_version_hash`. Such hash is used in `load_schema` to
detect if import schema changed and thus to overwrite the storage schema.

If export schema path is configured, `schema` will be exported to it.

### save\_import\_schema\_if\_not\_exists

```python
def save_import_schema_if_not_exists(schema: Schema) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/schema_storage.py#L80)

Saves import schema, if not exists. If schema was saved, link itself as imported from

