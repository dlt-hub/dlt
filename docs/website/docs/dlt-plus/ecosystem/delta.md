---
title: delta
description: Delta destination documentation
---

# Delta

The `delta` destination provides additional features on top of the `filesystem` [destination](../../dlt-ecosystem/destinations/filesystem) in OSS `dlt`. This page only documents the additional features—use the documentation provided in OSS dlt for standard functionality.

## Table format
`delta` destination automatically assigns `delta` table format to all resources that it will load. You can still
fall back to storing files (as specified in `file_format`) by setting `table_format` to **native** on a resource.

## Configuration
Iceberg destinations looks for its configuration under **destination.delta**. Otherwise it is configured
in the same way as `filesystem` destination.

```toml
[destination.delta]
bucket_url = "s3://[your_bucket_name]" # replace with your bucket name,

[destination.iceberg.credentials]
aws_access_key_id = "please set me up!" # copy the access key here
aws_secret_access_key = "please set me up!" # copy the secret access key here
```

You are still able to use regular filesystem configuration.
```py
from dlt_plus.destinations import delta

dest_ = delta(destination_name="filesystem")
```
