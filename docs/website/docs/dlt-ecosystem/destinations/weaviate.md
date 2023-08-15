---
title: Weaviate
description: Weaviate is an open source vector database that can be used as a destination in the DLT.
keywords: [weaviate, vector database, destination, dlt]
---

# Weaviate

Weaviate is an open source vector database. It allows you to store data objects and perform similarity searches over them.
This destination helps you to load data into Weaviate from [dlt resources](../../general-usage/resource.md).

## Quickstart

1. To use Weaviate as a destination, make sure the dlt is installed with 'weaviate' extra:

```bash
pip install dlt[weaviate]
```

2. Next, configure the destination in the dlt secrets file. The file is located at `~/.dlt/secrets.toml` by default. Add the following section to the secrets file:

```toml
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
- `tokenization`: the dictionary containing the tokenization configuration for a field. The dictionary should have the following structure `{'field_name': 'method'}`. Valid methods are "word", "lowercase", "whitespace", "field". The default is "word".

A more comprehensive pipeline would load data from some API or use one of dlt's [verified sources](../verified-sources/).

## Write dispotition

All write dispositions are supported.

For `merge` disposition you would need to specify a `primary_key` for the resource:

```python
@dlt.resource(primary_key="id", write_disposition="merge")
def my_resource():
    ...
```
