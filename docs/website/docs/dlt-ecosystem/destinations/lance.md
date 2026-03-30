---
title: Lance
description: Lance is an open-source columnar format for AI/ML that can be used as a destination in dlt.
keywords: [lance, lakehouse, vector database, destination, dlt, embeddings, branching]
---

# Lance

[Lance](https://lance.org) is an open-source columnar data format designed for AI/ML workloads, with native support for versioning, zero-copy access, and fast vector search. The `lance` destination lets you load data into Lance datasets stored on local disk or cloud object storage (S3, Azure, GCS).

Optionally, the destination can generate **vector embeddings** using the [LanceDB](https://lancedb.com/) embedding functions library.


<!--@@@DLT_DESTINATION_CAPABILITIES lance-->

:::note Lance vs. LanceDB destination
dlt ships two Lance-related destinations:

- **`lance`** (this page) — stores data on local disk or cloud object storage (S3, GCS, Azure). Uses the [`lance`](https://github.com/lancedb/lance) library for table management and, optionally, the [`lancedb`](https://github.com/lancedb/lancedb) library for embedding generation.
- **`lancedb`** ([docs](lancedb.md)) — stores data locally or on [LanceDB Cloud](https://docs.lancedb.com/cloud/index). Uses the `lancedb` library exclusively for all operations.

The `lancedb` destination will be phased out in favor of `lance`.
:::

## Setup guide

### Install dlt with `lance` dependencies

```sh
pip install "dlt[lance]"
```

### Quick start

```py
import dlt

movies = [
    {"id": 1, "title": "Blade Runner", "year": 1982},
    {"id": 2, "title": "Ghost in the Shell", "year": 1995},
    {"id": 3, "title": "The Matrix", "year": 1999},
]

pipeline = dlt.pipeline(
    pipeline_name="movies",
    destination="lance",
    dataset_name="movies_db",
)

info = pipeline.run(movies, table_name="movies")
```

To add vector embeddings, wrap your data with `lance_adapter` — see [Embeddings](#embeddings-configuration) below.

## Storage configuration

Configure storage in `~/.dlt/config.toml` (or secrets/environment variables). The `bucket_url` determines the storage backend.

### Local storage (default)

If `bucket_url` is not configured, the current working directory is used.

```toml
[destination.lance.storage]
bucket_url = "/my/dir"
```

### Cloud storage

Cloud credentials use the same configuration fields as the [filesystem destination](filesystem.md#set-up-the-destination-and-credentials), just under `destination.lance.storage` instead of `destination.filesystem`. Under the hood, credentials are passed to the [object_store](https://docs.rs/object_store/latest/object_store/) Rust crate (not `fsspec`), so some filesystem-specific options are not supported.

#### Amazon S3

```toml
[destination.lance.storage]
bucket_url = "s3://my-bucket"

[destination.lance.storage.credentials]
aws_access_key_id = "AKIA..."
aws_secret_access_key = "..."
region_name = "us-east-1"
```

#### Google Cloud Storage

```toml
[destination.lance.storage]
bucket_url = "gs://my-bucket"

[destination.lance.storage.credentials]
project_id = "my-project"
client_email = "...@...iam.gserviceaccount.com"
private_key = "-----BEGIN RSA PRIVATE KEY-----\n..."
```

#### Azure Blob Storage

```toml
[destination.lance.storage]
bucket_url = "az://my-container"

[destination.lance.storage.credentials]
azure_storage_account_name = "myaccount"
azure_storage_account_key = "..."
```

### Additional storage options

You can pass storage-specific options (e.g. endpoint overrides, timeouts) via the `options` dict. See the [Lance Object Store Configuration](https://lance.org/guide/object_store/) docs for all available options.

```toml
[destination.lance.storage]
bucket_url = "s3://my-bucket"

[destination.lance.storage.options]
allow_http = "true"
```

## Namespace architecture

The `lance` destination uses the [Lance Directory Namespace](https://lance.org/format/namespace/dir/catalog-spec/) (V2 Catalog Spec) to organize tables. The root namespace is a physical directory on disk. Child namespaces (e.g. per dataset) are logical groupings tracked by the catalog (`__manifest/`).

The logical layout is:

```text
bucket_url/
└── namespace_name/                ← root namespace directory (default: "dlt_lance_namespace")
    ├── __manifest/                ← catalog tracking namespaces and tables
    ├── <hash>_<dataset>$movies/   ← lance table data
    ├── <hash>_<dataset>$_dlt_version/
    └── ...
```

- **Root namespace** — a physical directory at `bucket_url/namespace_name`. The `namespace_name` defaults to `"dlt_lance_namespace"` and can be set to `""` to use `bucket_url` directly.
- **Dataset namespace** — a logical child namespace named after `dataset_name`, tracked in the `__manifest/` catalog. Created automatically when the pipeline runs. All tables for the dataset are registered inside it.
- **Tables** — stored as hash-prefixed directories at the root namespace level, not nested under a dataset subdirectory.

```toml
[destination.lance.storage]
bucket_url = "s3://my-bucket"
namespace_name = "production"  # root namespace subdirectory
```

## Branching

Lance datasets support [branches](https://lance.org/guide/tags_and_branches/) — lightweight version pointers for isolated reads and writes. Configure a branch name to direct all pipeline operations to that branch:

```toml
[destination.lance.storage]
branch_name = "staging"
```

Or in Python:

```py
from dlt.destinations.impl.lance.configuration import LanceStorageConfiguration

pipeline = dlt.pipeline(
    destination=dlt.destinations.lance(
        storage=LanceStorageConfiguration(branch_name="staging"),
    ),
    dataset_name="my_data",
)
```

When `branch_name` is not set, the default `main` branch is used. Branches are created automatically on first write if they don't exist.

Branching is dataset-wide — all tables, including dlt system tables (`_dlt_version`, `_dlt_loads`, `_dlt_pipeline_state`), are read from and written to the configured branch. This means each branch maintains its own pipeline state, schema history, and load metadata, providing full isolation between branches. Schemas can evolve independently in different branches.

## Write dispositions

All [write dispositions](../../general-usage/incremental-loading.md#choosing-a-write-disposition) are supported.

### Append

The default. Inserts all records without updating or deleting existing data.

### Replace

Replaces all data in the table using a truncate-and-insert strategy:

```py
info = pipeline.run(movies, table_name="movies", write_disposition="replace")
```

### Merge (upsert)

Updates existing records and inserts new ones based on a unique identifier. Use `lance_adapter` to specify the `merge_key`:

```py
from dlt.destinations.adapters import lance_adapter

pipeline.run(
    lance_adapter(data, merge_key="doc_id"),
    write_disposition={"disposition": "merge", "strategy": "upsert"},
    primary_key=["doc_id", "chunk_id"],
)
```

The `merge_key` identifies the parent document. If `merge_key` is not specified, the first element of `primary_key` is used as fallback. When orphan removal is enabled (the default), only a single merge key is supported because the orphan deletion filter operates on a single column. To use compound merge keys, disable orphan removal with `remove_orphans=False`.

#### Orphan removal

By default, when parent documents are updated or deleted during a merge, orphaned child records (chunks that no longer have a matching parent) are automatically removed. To disable this:

```py
lance_adapter(data, merge_key="doc_id", remove_orphans=False)
```

## Embeddings configuration

To generate vector embeddings automatically, configure an embedding provider. The embedding generation is powered by the [LanceDB embedding functions](https://docs.lancedb.com/embedding/index#embedding-model-providers) library.

```toml
[destination.lance.embeddings]
provider = "openai"
name = "text-embedding-3-small"
vector_column = "vector"
max_retries = 3

[destination.lance.embeddings.credentials]
api_key = "sk-..."
```

Any additional provider-specific arguments can be passed via `kwargs`:

```toml
[destination.lance.embeddings.kwargs]
api_base = "https://my-proxy.example.com/v1"
```

Then use `lance_adapter` to specify which columns should be embedded. The destination automatically adds a column named after `vector_column` (default: `"vector"`) to store the generated embeddings:

```py
from dlt.destinations.adapters import lance_adapter

info = pipeline.run(
    lance_adapter(movies, embed=["title", "description"]),
    table_name="movies",
)
```

## Access loaded data

### Standard dataset access

You can query loaded data using dlt's [dataset access](../../general-usage/dataset-access/dataset) interface, which works the same way as with any other destination:

```py
dataset = pipeline.dataset()
df = dataset["movies"].df()
```

### Low-level Lance access

For operations specific to the Lance format — such as version management, tagging, or direct reads — use `open_lance_dataset` on the destination client. It returns a `lance.LanceDataset` from the [lance](https://github.com/lancedb/lance) library:

```py
with pipeline.destination_client() as client:
    ds = client.open_lance_dataset("movies")  # type: ignore[attr-defined]
    ds.create_tag("v1.0")
    print(ds.tags())
```

You can also check out a specific branch or version:

```py
with pipeline.destination_client() as client:
    ds = client.open_lance_dataset("movies", branch_name="staging", version_number=5)  # type: ignore[attr-defined]
```

### LanceDB vector search

For vector similarity search and other LanceDB-specific features, use `open_lancedb_table`. It returns a `lancedb.table.LanceTable` from the [lancedb](https://github.com/lancedb/lancedb) library:

```py
with pipeline.destination_client() as client:
    tbl = client.open_lancedb_table("movies")  # type: ignore[attr-defined]
    results = tbl.search("sci-fi classic").limit(5).to_list()
```

## dbt support

The Lance destination does not support dbt integration.

## Syncing of `dlt` state

The Lance destination supports syncing of the `dlt` state.

<!--@@@DLT_TUBA lance-->
