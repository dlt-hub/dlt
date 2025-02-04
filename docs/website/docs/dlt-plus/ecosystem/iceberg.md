---
title: Iceberg destination
description: filesystem destination documentation
---

# Filesystem

The `filesystem` destination provides additional features on top of the `filesystem` [destination](../../dlt-ecosystem/destinations/filesystem) in OSS `dlt`. This page only documents the additional features—use the documentation provided in OSS dlt for standard functionality.

## `delete-insert` merge strategy with `iceberg` table format
The `delete-insert` [merge strategy](../../general-usage/incremental-loading#delete-insert-strategy) can be used when using the `iceberg` [table format](../../dlt-ecosystem/destinations/delta-iceberg):

```py
@dlt.resource(
    primary_key="id",  # merge_key also works; primary_key and merge_key may be used together
    write_disposition={"disposition": "merge", "strategy": "delete-insert"},
    table_format="iceberg"
)
def my_resource():
    yield [
        {"id": 1, "foo": "foo"},
        {"id": 2, "foo": "bar"}
    ]
...
```

### Known limitations
- Compound keys are not supported: use a single `primary_key` **and/or** a single `merge_key`.
  - As a workaround, you can [transform](../../general-usage/resource#filter-transform-and-pivot-data) your resource data with `add_map` to add a new column that contains a hash of the key columns, and use that column as `primary_key` or `merge_key`.
- Nested tables are not supported: avoid complex data types or [disable nesting](../../general-usage/source#reduce-the-nesting-level-of-generated-tables)