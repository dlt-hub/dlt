---
title: iceberg
description: Iceberg destination documentation
---

# Iceberg

The `iceberg` destination provides additional features on top of the `filesystem` [destination](../../dlt-ecosystem/destinations/filesystem) in OSS `dlt`. This page only documents the additional features—use the documentation provided in OSS dlt for standard functionality.

## `delete-insert` merge strategy with `iceberg` table format
The `delete-insert` [merge strategy](../../general-usage/incremental-loading#delete-insert-strategy) can be used when using the `iceberg` [table format](../../dlt-ecosystem/destinations/delta-iceberg):

```py
@dlt.resource(
    primary_key="id",  # merge_key also works; primary_key and merge_key may be used together
    write_disposition={"disposition": "merge", "strategy": "delete-insert"},
)
def my_resource():
    yield [
        {"id": 1, "foo": "foo"},
        {"id": 2, "foo": "bar"}
    ]
...

pipeline = dlt.pipeline("loads_iceberg", destination="iceberg")

```

## Table format
`iceberg` destination automatically assigns `iceberg` table format to all resources that it will load. You can still
fall back to storing files (as specified in `file_format`) by setting `table_format` to **native** on a resource.

## Configuration
Iceberg destinations looks for its configuration under **destination.iceberg**. Otherwise it is configured
in the same way as `filesystem` destination.

```toml
[destination.iceberg]
bucket_url = "s3://[your_bucket_name]" # replace with your bucket name,

[destination.iceberg.credentials]
aws_access_key_id = "please set me up!" # copy the access key here
aws_secret_access_key = "please set me up!" # copy the secret access key here
```

You are still able to use regular filesystem configuration.
```py
from dlt_plus.destinations import iceberg

dest_ = iceberg(destination_name="filesystem")
```


### Known limitations
- Compound keys are not supported: use a single `primary_key` **and/or** a single `merge_key`.
  - As a workaround, you can [transform](../../general-usage/resource#filter-transform-and-pivot-data) your resource data with `add_map` to add a new column that contains a hash of the key columns, and use that column as `primary_key` or `merge_key`.
- Nested tables are not supported: avoid complex data types or [disable nesting](../../general-usage/source#reduce-the-nesting-level-of-generated-tables)