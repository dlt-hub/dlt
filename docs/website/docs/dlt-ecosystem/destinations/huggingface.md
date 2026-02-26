---
title: Hugging Face Datasets
description: Load data into Hugging Face Datasets repositories using dlt
keywords: [hugging face, hf, datasets, parquet, filesystem, hub]
---

# Hugging Face Datasets

The Hugging Face destination loads data into [Hugging Face Datasets](https://huggingface.co/docs/datasets/index) repositories. It is built on top of the [filesystem](filesystem) destination and uses the `hf://` protocol to write [Parquet](../file-formats/parquet) files to the Hugging Face Hub.

:::note
Because this destination extends `filesystem`, most filesystem concepts (file layouts, write dispositions, dlt state sync, etc.) apply here. This page covers the Hugging Face-specific behavior and configuration. Refer to the [filesystem destination docs](filesystem) for the full feature set.
:::

## Install dlt with Hugging Face support

```sh
pip install "dlt[hf]"
```

This installs the `huggingface_hub` package alongside `dlt`.

## Initialize the dlt project

```sh
dlt init chess filesystem
```

This creates a sample pipeline with the `filesystem` destination. To use Hugging Face, update the `bucket_url` in `.dlt/secrets.toml` to use the `hf://` scheme as shown below.

## Set up credentials and destination

### bucket_url

The `bucket_url` uses the `hf://datasets/<namespace>` scheme, where `<namespace>` is your Hugging Face username or organization name.

Each dlt dataset becomes a **separate Hugging Face dataset repository** under that namespace. For example, loading to the dataset name `chess_data` with namespace `my-org` creates the repository `my-org/chess_data`.

```toml
[destination.filesystem]
bucket_url = "hf://datasets/my-org"    # replace "my-org" with your username or org
```

### Authentication

Configure your [Hugging Face User Access Token](https://huggingface.co/docs/hub/security-tokens) in `.dlt/secrets.toml`:

```toml
[destination.filesystem.credentials]
hf_token = "hf_..."    # replace with your Hugging Face User Access Token
```

Instead of setting `hf_token` in config, you can authenticate by:

- Setting the `HF_TOKEN` [environment variable](#hugging-face-environment-variables)
- Using a [locally saved token](https://huggingface.co/docs/huggingface_hub/en/quick-start#login-command) created with `huggingface-cli login`

Authentication is attempted in that order of priority: `hf_token` config → `HF_TOKEN` env var → locally saved token.

### Private Hub endpoint

By default, `https://huggingface.co` is used as the API endpoint. To use a [Private Hub](https://huggingface.co/docs/hub/enterprise-hub) or a self-hosted endpoint, set `hf_endpoint`:

```toml
[destination.filesystem.credentials]
hf_endpoint = "https://your-private-hub.example.com"
```

### Full example configuration

```toml
[destination.filesystem]
bucket_url = "hf://datasets/my-org"

[destination.filesystem.credentials]
hf_token = "hf_..."
# hf_endpoint = "https://your-private-hub.example.com"  # optional
```

## Write disposition

The Hugging Face destination supports two write dispositions:

- `append` — new data files are added to the dataset repository
- `replace` — existing data files for the table are deleted, then the new files are added

:::warning
`merge` write disposition is **not supported** for the Hugging Face destination. Pipelines using `merge` will fall back to `append` with a warning.
:::

## File format

The Hugging Face destination **always uses [Parquet](../file-formats/parquet)** as the file format, regardless of other configuration. This is required because the [Hugging Face dataset viewer](https://huggingface.co/docs/dataset-viewer/index) needs Parquet files to preview datasets on the Hub.

The Parquet files are written with:
- **Page index** ([Apache Parquet page index](https://github.com/apache/parquet-format/blob/master/PageIndex.md)) for efficient column statistics and skipping
- **Content-defined chunking** for [CDC (Change Data Capture)](https://huggingface.co/blog/parquet-cdc) support

## Table formats

:::warning
The Hugging Face destination does **not** support [Delta](./delta-iceberg) or [Iceberg](./iceberg) table formats.
:::

## Files layout

The Hugging Face destination uses the same layout system as the [filesystem destination](filesystem#files-layout). The default layout is:

```text
{table_name}/{load_id}.{file_id}.{ext}
```

You can customize the layout using the same `layout` and `extra_placeholders` settings as the filesystem destination. See [Files layout](filesystem#files-layout) for all available placeholders and examples.

```toml
[destination.filesystem]
bucket_url = "hf://datasets/my-org"
layout = "{table_name}/{load_id}.{file_id}.{ext}"
```

## Hugging Face-specific behavior

### Dataset repositories

Each dlt dataset creates or updates a Hugging Face dataset repository (not a directory). The repository name is `<namespace>/<dataset_name>`, where `<namespace>` comes from the `bucket_url` and `<dataset_name>` is the pipeline's `dataset_name`.

### Atomic commits

All data files for a table and its child tables are committed to the Hub in a single git commit via the `HfApi` client. This minimizes the number of commits, avoids hitting Hugging Face [rate limits](https://huggingface.co/docs/hub/rate-limits#rate-limit-tiers), and prevents commit conflicts.

### Parquet with page index and CDC

The `hf` protocol defaults to `parquet` file format and always writes files with [page index](https://github.com/apache/parquet-format/blob/master/PageIndex.md) and [CDC](https://huggingface.co/blog/parquet-cdc) support. This enables efficient column statistics and skipping, and is required for the Hugging Face [dataset viewer](https://huggingface.co/docs/dataset-viewer/index) to preview datasets on the Hub.

### Dual client

`dlt` uses two Hugging Face clients together:

- **`HfApi`** — used for repository management (create/delete repo) and atomic commits
- **`HfFileSystem`** (fsspec) — used for file reads and DuckDB data access

The fsspec cache is invalidated after each `HfApi` mutation to keep the two clients consistent.

## Syncing dlt state

The Hugging Face destination fully supports [dlt state sync](../../general-usage/state#syncing-state-with-destination). Special files and folders (e.g., `_dlt_loads`, `_dlt_pipeline_state`) are created in the dataset repository to track pipeline state, schemas, and completed loads. These folders do not follow the `layout` setting.

By default, the last 100 pipeline state files are retained. You can configure this with `max_state_files`:

```toml
[destination.filesystem]
max_state_files = 100  # set to 0 or negative to disable cleanup
```

## Hugging Face environment variables

You can set Hugging Face [environment variables](https://huggingface.co/docs/huggingface_hub/package_reference/environment_variables) to configure the `huggingface_hub` library that `dlt` uses under the hood. For example, to increase the upload timeout:

```sh
HF_HUB_DOWNLOAD_TIMEOUT="30" python run_my_dlt_pipe.py
```

## Data access

The Hugging Face destination inherits the `sql_client` from the filesystem destination, which provides read-only SQL access to Parquet files using a DuckDB dialect. This also enables [`pipeline.dataset()`](../../general-usage/dataset-access/dataset), giving you Python-native access to loaded data as Pandas DataFrames, PyArrow tables, or Python tuples. See [filesystem data access](filesystem#data-access) for details.

## Example pipeline

```py
import dlt

@dlt.resource
def my_data():
    yield [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

pipeline = dlt.pipeline(
    pipeline_name="my_pipeline",
    destination="filesystem",
    dataset_name="my_dataset",
)

pipeline.run(my_data(), write_disposition="append")
```

With the `hf://` bucket URL configured, this creates or updates the `my-org/my_dataset` repository on Hugging Face with a Parquet file under `my_data/`.

## Troubleshooting

### Rate limit errors

Hugging Face enforces [rate limits](https://huggingface.co/docs/hub/rate-limits#rate-limit-tiers) on repository commits. The `dlt` HF client automatically batches all file writes for a table into a single commit to minimize commit frequency. If you still hit rate limits, consider reducing the pipeline frequency or upgrading your Hugging Face plan.

### Authentication errors

If you see authentication errors, verify that:

1. Your token has **write** access to the target namespace.
2. The token is correctly set in `hf_token`, `HF_TOKEN`, or via `huggingface-cli login`.
3. If using a Private Hub, `hf_endpoint` points to the correct URL.
4. Dataset `dataset_name` exists.
