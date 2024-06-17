---
title: LanceDB
description: LanceDB is an open source vector database that can be used as a destination in dlt.
keywords: [ lancedb, vector database, destination, dlt ]
---

# LanceDB

[LanceDB](https://lancedb.com/) is an open-source, high-performance vector database. It allows you to store data objects and perform similarity searches over them.
This destination helps you load data into LanceDB from [dlt resources](../../general-usage/resource.md).

## Setup Guide

### Choosing a Model Provider

First, you need to decide which embedding model provider to use. You can find all supported providers by visiting the official [LanceDB docs](https://lancedb.github.io/lancedb/embeddings/default_embedding_functions/).


### Install dlt with LanceDB

To use LanceDB as a destination, make sure `dlt` is installed with the `lancedb` extra:

```sh
pip install "dlt[lancedb]"
```

the lancedb extra only installs `dlt` and `lancedb`. You will need to install your model provider's SDK.

You can find which libraries you need to also referring to the [LanceDB docs](https://lancedb.github.io/lancedb/embeddings/default_embedding_functions/).

### Configure the destination

Configure the destination in the dlt secrets file located at `~/.dlt/secrets.toml` by default. Add the following section:

```toml
[destination.lancedb]
embedding_model_provider = "cohere"
embedding_model = "embed-english-v3.0"
[destination.lancedb.credentials]
uri = ".lancedb"
api_key = "api_key"
embedding_model_provider_api_key = "embedding_model_provider_api_key"
```

- The `uri` specifies the location of your LanceDB instance. It defaults to a local, on-disk instance if not provided.
- The `api_key` is required for remote LanceDB cloud connections. If you are using a local instance, you can ignore this configuration key and leave as is.
- The `embedding_model_provider` specifies the embedding provider used for generating embeddings. The default is `cohere`.
- The `embedding_model` specifies the model used by the embedding provider for generating embeddings.
  Check with the embedding provider which options are available.
  Reference https://lancedb.github.io/lancedb/embeddings/default_embedding_functions/.
- The `embedding_model_provider_api_key` is the API key for the embedding model provider used to generate embeddings.

:::info Available Model Providers
- "gemini-text"
- "bedrock-text"
- "cohere"
- "gte-text"
- "imagebind"
- "instructor"
- "open-clip"
- "openai"
- "sentence-transformers"
- "huggingface"
- "colbert"
:::

### Define your data source

For example:

```py
import dlt
from dlt.destinations.adapters import lancedb_adapter


movies = [
  {
    "id": 1,
    "title": "Blade Runner",
    "year": 1982,
  },
  {
    "id": 2,
    "title": "Ghost in the Shell",
    "year": 1995,
  },
  {
    "id": 3,
    "title": "The Matrix",
    "year": 1999,
  },
]
```

### Create a pipeline:

```py
pipeline = dlt.pipeline(
  pipeline_name="movies",
  destination="lancedb",
  dataset_name="MoviesDataset",
)
```

### Run the pipeline:

```py
info = pipeline.run(
  lancedb_adapter(
    movies,
    embed="title",
  )
)
```

The data is now loaded into LanceDB.

To use **vector search** after loading, you **must specify which fields LanceDB should generate embeddings for**. Do this by wrapping the data (or dlt resource) with the **`lancedb_adapter`**
function.

## Using an Adapter to Specify Columns to Vectorise

The `lancedb_adapter` is a helper function that configures the resource for the LanceDB destination:

```py
lancedb_adapter(data, embed)
```

It accepts the following arguments:

- `data`: a dlt resource object, or a Python data structure (e.g. a list of dictionaries).
- `embed`: a name of the field or a list of names to generate embeddings for.

Returns: [dlt resource](../../general-usage/resource.md) object that you can pass to the `pipeline.run()`.

Example:

```py
lancedb_adapter(
  resource,
  embed=["title", "description"],
)
```

## Write disposition

All [write dispositions](../../general-usage/incremental-loading.md#choosing-a-write-disposition) are supported by the LanceDB destination.

### Replace

The [replace](../../general-usage/full-loading.md) disposition replaces the data in the destination with the data from the resource.

```py
info = pipeline.run(
  lancedb_adapter(
    movies,
    embed="title",
  ),
  write_disposition="replace",
)
```

### Merge

The [merge](../../general-usage/incremental-loading.md) write disposition merges the data from the resource with the data at the destination based on a unique identifier.

```py
pipeline.run(
  lancedb_adapter(
    movies,
    embed="title",
  ),
  write_disposition="merge",
  primary_key="id",
)
```

### Append

This is the default disposition. It will append the data to the existing data in the destination.

## Additional Destination Options

- `dataset_separator`: The character used to separate the dataset name from table names. Defaults to "___".
- `vector_field_name`: The name of the special field to store vector embeddings. Defaults to "vector__".
- `id_field_name`: The name of the special field used for deduplication and merging. Defaults to "id__".
- `max_retries`: The maximum number of retries for embedding operations. Set to 0 to disable retries. Defaults to 3.


## dbt support

The LanceDB destination doesn't support dbt integration.

## Syncing of `dlt` state

The LanceDB destination supports syncing of the `dlt` state.

## Current Limitations

Adding new fields to an existing LanceDB table requires loading the entire table data into memory as a PyArrow table.
This is because PyArrow tables are immutable, so adding fields requires creating a new table with the updated schema.

For huge tables, this may impact performance and memory usage since the full table must be loaded into memory to add the new fields.
Keep these considerations in mind when working with large datasets and monitor memory usage if adding fields to sizable existing tables.

<!--@@@DLT_TUBA lancedb-->

