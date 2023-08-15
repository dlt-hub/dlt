---
title: Weaviate
description: Weaviate is an open source vector database that can be used as a destination in the DLT.
keywords: [weaviate, vector database, destination, dlt]
---

# Weaviate

Weaviate is an open source vector database. It allows you to store data objects and perform similarity searches over them.
This destination helps you to load data into Weaviate from [dlt resources](../../general-usage/resource.md).

## Setup Guide

1. To use Weaviate as a destination, make sure the dlt is installed with 'weaviate' extra:

```bash
pip install dlt[weaviate]
```

2. Next, configure the destination in the dlt secrets file. The file is located at `~/.dlt/secrets.toml` by default. Add the following section to the secrets file:

```toml title="~/.dlt/secrets.toml"
[destination.weaviate.credentials]
url = "https://your-weaviate-url"
api_key = "your-weaviate-api-key"

[destination.weaviate.credentials.additional_headers]
X-OpenAI-Api-Key = "your-openai-api-key"
```

In this setup guide, we are using the [Weaviate Cloud Services](https://console.weaviate.cloud/) to get a Weaviate instance and [OpenAI API](https://platform.openai.com/) for generating embeddings.

3. Define the source of the data. For starters, let's load some data from a simple data structure:

```python
import dlt
from dlt.destinations.weaviate.weaviate_adapter import weaviate_adapter

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

```python
pipeline = dlt.pipeline(
    pipeline_name="movies",
    destination="weaviate",
    dataset_name="MoviesDataset",
)
```

5. Run the pipeline:

```python
info = pipeline.run(
    weaviate_adapter(
        movies,
        vectorize="title",
    )
)
```

6. Check the results:

```python
print(info)
```

The data is now loaded into Weaviate.

## weaviate_adapter

The `weaviate_adapter` is a helper function that configures the resource for the Weaviate destination:

```python
weaviate_adapter(data, vectorize, tokenization)
```

It accepts the following arguments:
- `data`: a dlt resource object or a Python data structure (e.g. a list of dictionaries).
- `vectorize`: a name of the field or a list of names that should be vectorized by Weaviate.
- `tokenization`: the dictionary containing the tokenization configuration for a field. The dictionary should have the following structure `{'field_name': 'method'}`. Valid methods are "word", "lowercase", "whitespace", "field". The default is "word". See [Property tokenization](https://weaviate.io/developers/weaviate/config-refs/schema#property-tokenization) in Weaviate documentation for more details.

Example:

```python
weaviate_adapter(
    resource,
    vectorize=["title", "description"],
    tokenization={"title": "word", "description": "whitespace"},
)
```

:::tip

A more comprehensive pipeline would load data from some API or use one of dlt's [verified sources](../verified-sources/).

:::

## Write dispotition

A [write disposition](../../general-usage/incremental-loading.md#choosing-a-write-disposition) defines how the data should be written to the destination. All write dispositions are supported by the Weaviate destination.

### Replace

The [`replace`](../../general-usage/full-loading.md) disposition replaces the data in the destination with the data from the resource. It deletes all the classes and objects and recreates the schema before loading the data.

In the movie example from the setup guide, we can use the `replace` disposition to reload the data every time we run the pipeline:

```python
info = pipeline.run(
    weaviate_adapter(
        movies,
        vectorize="title",
    ),
    write_disposition="replace",
)
```

### Merge

The [`merge`](../../general-usage/incremental-loading.md) disposition merges the data from the resource with the data in the destination.
For `merge` disposition you would need to specify a `primary_key` for the resource:

```python
info = pipeline.run(
    weaviate_adapter(
        movies,
        vectorize="title",
    ),
    primary_key="document_id",
    write_disposition="merge"
)
```

Internally dlt will use `primary_key` (`document_id` in the example above) to generate a unique identifier ([UUID](https://weaviate.io/developers/weaviate/manage-data/create#id)) for each object in Weaviate. If the object with the same UUID already exists in Weaviate, it will be updated with the new data. Otherwise, a new object will be created.

### Append

This is the default disposition. It will append the data to the existing data in the destination ignoring the `primary_key` field.

## Data loading

### Data types
### Dataset name
### Class and property names normalization

## Additional destination options

### dbt support

### Syncing of `dlt` state
