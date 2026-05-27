---
title: Raw Data Export
description: Copy files and data to object storage as a side-effect during extraction
keywords: [raw data export, file copy, backup, object storage, fsspec, file mirror]
---

# Raw Data Export

`dlt.sources.raw_export` copies extracted data to object storage (S3, GCS, Azure, local
filesystem) as a side-effect in the extraction pipe chain. It sits after `Incremental` and
`LimitItem`, so it captures exactly what will be sent to the destination.

## Basic usage: file mirroring

Copy files from one storage location to another with incremental tracking:

```py
import dlt
from dlt.sources.filesystem import filesystem

@dlt.resource
def raw_files(
    modified=dlt.sources.incremental("modification_date"),
    export=dlt.sources.raw_export("s3://my-backups", name_path="relative_path"),
):
    yield from filesystem("s3://source-data/feeds/", file_glob="**/*.csv")

pipeline = dlt.pipeline(pipeline_name="file_mirror", destination="duckdb")
pipeline.run(raw_files())
```

Each file from the source is copied byte-for-byte to the backup location. The directory
structure is preserved via `name_path="relative_path"`. Only new or modified files are
copied (filtered by `Incremental`).

## Export-only mode

Use dlt as a pure incremental file copier — no destination load:

```py
@dlt.resource
def raw_files(
    modified=dlt.sources.incremental("modification_date"),
    export=dlt.sources.raw_export(
        "s3://dest-bucket",
        name_path="relative_path",
        export_only=True,
    ),
):
    yield from filesystem("s3://source-bucket/feeds/", file_glob="**/*")

pipeline = dlt.pipeline(pipeline_name="file_copier", destination="duckdb")
pipeline.run(raw_files())
```

With `export_only=True`, items are consumed after copying. No data reaches the normalizer
or destination. Incremental state is still persisted for tracking.

## Exporting API data as JSONL

Dict items from API sources are buffered and written as JSONL files:

```py
@dlt.resource
def events(
    updated_at=dlt.sources.incremental("updated_at"),
    export=dlt.sources.raw_export("s3://backup", name_path="event_date"),
):
    yield from paginated_api_call(after=updated_at.last_value)

pipeline = dlt.pipeline(pipeline_name="api_backup", destination="bigquery")
pipeline.run(events())
```

Files are organized by `name_path` value with run-scoped naming to prevent cross-run
overwrites: `s3://backup/.../events/2025-02-16/{load_id}.{file_id}.jsonl`.

## Exporting Arrow tables as Parquet

Arrow tables and DataFrames are written as Parquet files (one table = one file):

```py
import pyarrow as pa

@dlt.resource
def reports(
    export=dlt.sources.raw_export("s3://backup", name_path="month"),
):
    for month in ["2025-01", "2025-02"]:
        table = pa.table({"month": [month], "value": [100]})
        yield table
```

## Credentials

Credentials are resolved from `secrets.toml` or environment variables via dlt's
standard config system. Configure them the same way as for the filesystem destination:

```toml
[sources.raw_export.credentials]
aws_access_key_id = "your-key"
aws_secret_access_key = "your-secret"
```

Or pass credentials explicitly:

```py
from dlt.common.storages.configuration import AWSCredentials

creds = AWSCredentials(
    aws_access_key_id="your-key",
    aws_secret_access_key="your-secret",
)
export = dlt.sources.raw_export(
    "s3://backup",
    name_path="relative_path",
    credentials=creds,
)
```

## dev_mode behavior

By default, export is disabled in `dev_mode` to avoid polluting backup locations during
experimentation. To enable it, set `enable_in_dev_mode=True` — files are written to an
isolated `_dev{instance_id}/` path:

```py
export = dlt.sources.raw_export(
    "s3://backup",
    name_path="relative_path",
    enable_in_dev_mode=True,
)
```

## Reading back the export path

To access the resolved export path after a run, pass the exporter explicitly:

```py
exporter = dlt.sources.raw_export("s3://backup", name_path="relative_path")

@dlt.resource
def raw_files(export=exporter):
    yield from filesystem("s3://source/", file_glob="**/*")

pipeline.run(raw_files(export=exporter))
print(exporter.export_path)
print(exporter.fs_client)
```

The `fs_client` attribute gives you the `AbstractFileSystem` instance used for writing,
allowing you to list or read exported files programmatically.

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `bucket_url` | `str` | required | Base URL for destination storage (s3://, gs://, file://, etc.) |
| `name_path` | `str` | required | Field name in each item to derive the destination file path |
| `file_format` | `str` | `None` | Output format for structured data: `"jsonl"`, `"parquet"`, `"csv"`. Auto-detected if `None`. |
| `credentials` | `FileSystemCredentials` | `None` | Filesystem credentials. Resolved from secrets.toml / env vars if not provided. |
| `enable_in_dev_mode` | `bool` | `False` | Enable export during `dev_mode` (writes to isolated `_dev/` path) |
| `export_only` | `bool` | `False` | Consume items after export — no data reaches the destination |
