---
title: Qdrant
description: Qdrant is a high-performance vector search engine/database that can be used as a destination in dlt.
keywords: [qdrant, vector database, destination, dlt]
---

# Qdrant

[Qdrant](https://qdrant.tech/) is an open-source, high-performance vector search engine/database. It deploys as an API service, providing a search for the nearest high-dimensional vectors.
This destination helps you load data into Qdrant from [dlt resources](../../general-usage/resource.md).

## Setup guide

1. To use Qdrant as a destination, make sure `dlt` is installed with the `qdrant` extra:

```sh
pip install "dlt[qdrant]"
```

2. Next, configure the destination in the dlt secrets file. The file is located at `~/.dlt/secrets.toml` by default. Add the following section to the secrets file:

```toml
[destination.qdrant.credentials]
location = "https://your-qdrant-url"
api_key = "your-qdrant-api-key"
```

In this setup guide, we are using the [Qdrant Cloud](https://cloud.qdrant.io/) to get a hosted Qdrant instance and the [FastEmbed](https://github.com/qdrant/fastembed) package that is built into the [Qdrant client library](https://github.com/qdrant/qdrant-client) for generating embeddings.

If no configuration options are provided, the default fallback will be `http://localhost:6333` with no API key.

3. Define the source of the data. For starters, let's load some data from a simple data structure:

```py
import dlt
from dlt.destinations.adapters import qdrant_adapter

movies = [
    {
        "title": "Blade Runner",
        "year": 1982,
    },
    {
        "title": "Ghost in the Shell",
        "year": 1995,
    },
    {
        "title": "The Matrix",
        "year": 1999,
    }
]
```

4. Define the pipeline:

```py
pipeline = dlt.pipeline(
    pipeline_name="movies",
    destination="qdrant",
    dataset_name="MoviesDataset",
)
```

5. Run the pipeline:

```py
info = pipeline.run(
    qdrant_adapter(
        movies,
        embed="title",
    )
)
```

6. Check the results:

```py
print(info)
```

The data is now loaded into Qdrant.

To use vector search after the data has been loaded, you must specify which fields Qdrant needs to generate embeddings for. You do that by wrapping the data (or dlt resource) with the `qdrant_adapter` function.

## qdrant_adapter

The `qdrant_adapter` is a helper function that configures the resource for the Qdrant destination:

```py
qdrant_adapter(data, embed)
```

It accepts the following arguments:

- `data`: a dlt resource object or a Python data structure (e.g., a list of dictionaries).
- `embed`: a name of the field or a list of names to generate embeddings for.

Returns: [dlt resource](../../general-usage/resource.md) object that you can pass to the `pipeline.run()`.

Example:

```py
qdrant_adapter(
    resource,
    embed=["title", "description"],
)
```

When using the `qdrant_adapter`, it's important to apply it directly to resources, not to the whole source. Here's an example:

```py
products_tables = sql_database().with_resources("products", "customers")

pipeline = dlt.pipeline(
        pipeline_name="postgres_to_qdrant_pipeline",
        destination="qdrant",
    )

# apply adapter to the needed resources
qdrant_adapter(products_tables.products, embed="description")
qdrant_adapter(products_tables.customers, embed="bio")

info = pipeline.run(products_tables)
```

:::tip
A more comprehensive pipeline would load data from some API or use one of dlt's [verified sources](../verified-sources/).
:::

## Write disposition

A [write disposition](../../general-usage/incremental-loading.md#choosing-a-write-disposition) defines how the data should be written to the destination. All write dispositions are supported by the Qdrant destination.

### Replace

The [replace](../../general-usage/full-loading.md) disposition replaces the data in the destination with the data from the resource. It deletes all the classes and objects and recreates the schema before loading the data.

In the movie example from the [setup guide](#setup-guide), we can use the `replace` disposition to reload the data every time we run the pipeline:

```py
info = pipeline.run(
    qdrant_adapter(
        movies,
        embed="title",
    ),
    write_disposition="replace",
)
```

### Merge

The [merge](../../general-usage/incremental-loading.md) write disposition merges the data from the resource with the data at the destination.
For the `merge` disposition, you need to specify a `primary_key` for the resource:

```py
info = pipeline.run(
    qdrant_adapter(
        movies,
        embed="title",
    ),
    primary_key="document_id",
    write_disposition="merge"
)
```

Internally, dlt will use the `primary_key` (`document_id` in the example above) to generate a unique identifier (UUID) for each point in Qdrant. If the object with the same UUID already exists in Qdrant, it will be updated with the new data. Otherwise, a new point will be created.

:::caution

If you are using the merge write disposition, you must set it from the first run of your pipeline; otherwise, the data will be duplicated in the database on subsequent loads.

:::

### Append

This is the default disposition. It will append the data to the existing data in the destination, ignoring the `primary_key` field.

## Dataset name

Qdrant uses collections to categorize and identify data. To avoid potential naming conflicts, especially when dealing with multiple datasets that might have overlapping table names, dlt includes the dataset name in the Qdrant collection name. This ensures a unique identifier for every collection.

For example, if you have a dataset named `movies_dataset` and a table named `actors`, the Qdrant collection name would be `movies_dataset_actors` (the default separator is an underscore).

However, if you prefer to have class names without the dataset prefix, skip the `dataset_name` argument.

For example:

```py
pipeline = dlt.pipeline(
    pipeline_name="movies",
    destination="qdrant",
)
```

## Additional destination options

- `embedding_batch_size`: (int) The batch size for embedding operations. The default value is 32.

- `embedding_parallelism`: (int) The number of concurrent threads to run embedding operations. Defaults to the number of CPU cores.

- `upload_batch_size`: (int) The batch size for data uploads. The default value is 64.

- `upload_parallelism`: (int) The maximum number of concurrent threads to run data uploads. The default value is 1.

- `upload_max_retries`: (int) The number of retries to upload data in case of failure. The default value is 3.

- `options`: ([QdrantClientOptions](#qdrant-client-options)) An instance of the `QdrantClientOptions` class that holds various Qdrant client options.

- `model`: (str) The name of the FlagEmbedding model to use. See the list of supported models at [Supported Models](https://qdrant.github.io/fastembed/examples/Supported_Models/). The default value is "BAAI/bge-small-en".

### Qdrant client options

The `QdrantClientOptions` class provides options for configuring the Qdrant client.

- `port`: (int) The port of the REST API interface. The default value is 6333.

- `grpc_port`: (int) The port of the gRPC interface. The default value is 6334.

- `prefer_grpc`: (bool) If `true`, the client will prefer to use the gRPC interface whenever possible in custom methods. The default value is `false`.

- `https`: (bool) If `true`, the client will use the HTTPS (SSL) protocol. The default value is `true` if an API key is provided, otherwise `false`.

- `prefix`: (str) If set, it adds the specified `prefix` to the REST URL path. For example, setting it to "service/v1" will result in the REST API URL as `http://localhost:6333/service/v1/{qdrant-endpoint}`. Not set by default.

- `timeout`: (int) The timeout for REST and gRPC API requests. The default value is 5.0 seconds for REST and unlimited for gRPC.

- `host`: (str) The host name of the Qdrant service. If both the URL and host are `None`, it is set to `localhost`.

- `path`: (str) The persistence path for a local Qdrant instance. Not set by default.

### Run Qdrant locally

You can find the setup instructions to run Qdrant [here](https://qdrant.tech/documentation/quick-start/#download-and-run).

### Syncing of `dlt` state

Qdrant destination supports syncing of the `dlt` state.

<!--@@@DLT_TUBA qdrant-->

