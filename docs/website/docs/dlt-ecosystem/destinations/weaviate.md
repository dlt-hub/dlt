---
title: Weaviate
description: Weaviate is an open source vector database that can be used as a destination in dlt.
keywords: [weaviate, vector database, destination, dlt]
---

# Weaviate

[Weaviate](https://weaviate.io/) is an open-source vector database. It allows you to store data objects and perform similarity searches over them.
This destination helps you load data into Weaviate from [dlt resources](../../general-usage/resource.md).

## Setup guide

1. To use Weaviate as a destination, make sure dlt is installed with the 'weaviate' extra:

```sh
pip install "dlt[weaviate]"
```

2. Next, configure the destination in the dlt secrets file. The file is located at `~/.dlt/secrets.toml` by default. Add the following section to the secrets file:

```toml
[destination.weaviate.credentials]
url = "https://your-weaviate-url"
api_key = "your-weaviate-api-key"

[destination.weaviate.credentials.additional_headers]
X-OpenAI-Api-Key = "your-openai-api-key"
```

In this setup guide, we are using the [Weaviate Cloud Services](https://console.weaviate.cloud/) to get a Weaviate instance and [OpenAI API](https://platform.openai.com/) for generating embeddings through the [text2vec-openai](https://weaviate.io/developers/weaviate/modules/retriever-vectorizer-modules/text2vec-openai) module.

You can host your own Weaviate instance using Docker Compose, Kubernetes, or embedded. Refer to Weaviate's [How-to: Install](https://weaviate.io/developers/weaviate/installation) or [dlt recipe we use for our tests](#run-weaviate-fully-standalone). In that case, you can skip the credentials part altogether:

```toml
[destination.weaviate.credentials.additional_headers]
X-OpenAI-Api-Key = "your-openai-api-key"
```
The `url` will default to **http://localhost:8080** and `api_key` is not defined - which are the defaults for the Weaviate container.


3. Define the source of the data. For starters, let's load some data from a simple data structure:

```py
import dlt
from dlt.destinations.adapters import weaviate_adapter

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
    destination="weaviate",
    dataset_name="MoviesDataset",
)
```

5. Run the pipeline:

```py
info = pipeline.run(
    weaviate_adapter(
        movies,
        vectorize="title",
    )
)
```

6. Check the results:

```py
print(info)
```

The data is now loaded into Weaviate.

Weaviate destination is different from other [dlt destinations](../destinations/). To use vector search after the data has been loaded, you must specify which fields Weaviate needs to include in the vector index. You do that by wrapping the data (or dlt resource) with the `weaviate_adapter` function.

## Weaviate adapter

The `weaviate_adapter` is a helper function that configures the resource for the Weaviate destination:

```py
weaviate_adapter(data, vectorize, tokenization)
```

It accepts the following arguments:
- `data`: a dlt resource object or a Python data structure (e.g., a list of dictionaries).
- `vectorize`: a name of the field or a list of names that should be vectorized by Weaviate.
- `tokenization`: the dictionary containing the tokenization configuration for a field. The dictionary should have the following structure `{'field_name': 'method'}`. Valid methods are "word", "lowercase", "whitespace", "field". The default is "word". See [Property tokenization](https://weaviate.io/developers/weaviate/config-refs/schema#property-tokenization) in Weaviate documentation for more details.

Returns: a [dlt resource](../../general-usage/resource.md) object that you can pass to the `pipeline.run()`.

Example:

```py
weaviate_adapter(
    resource,
    vectorize=["title", "description"],
    tokenization={"title": "word", "description": "whitespace"},
)
```
When using the `weaviate_adapter`, it's important to apply it directly to resources, not to the whole source. Here's an example:

```py
products_tables = sql_database().with_resources("products", "customers")

pipeline = dlt.pipeline(
        pipeline_name="postgres_to_weaviate_pipeline",
        destination="weaviate",
    )

# Apply adapter to the needed resources
weaviate_adapter(products_tables.products, vectorize="description")
weaviate_adapter(products_tables.customers, vectorize="bio")

info = pipeline.run(products_tables)
```

:::tip

A more comprehensive pipeline would load data from some API or use one of dlt's [verified sources](../verified-sources/).

:::

## Write disposition

A [write disposition](../../general-usage/incremental-loading.md#choosing-a-write-disposition) defines how the data should be written to the destination. All write dispositions are supported by the Weaviate destination.

### Replace

The [replace](../../general-usage/full-loading.md) disposition replaces the data in the destination with the data from the resource. It deletes all the classes and objects and recreates the schema before loading the data.

In the movie example from the [setup guide](#setup-guide), we can use the `replace` disposition to reload the data every time we run the pipeline:

```py
info = pipeline.run(
    weaviate_adapter(
        movies,
        vectorize="title",
    ),
    write_disposition="replace",
)
```

### Merge

The [merge](../../general-usage/incremental-loading.md) write disposition merges the data from the resource with the data in the destination.
For the `merge` disposition, you would need to specify a `primary_key` for the resource:

```py
info = pipeline.run(
    weaviate_adapter(
        movies,
        vectorize="title",
    ),
    primary_key="document_id",
    write_disposition="merge"
)
```

Internally, dlt will use `primary_key` (`document_id` in the example above) to generate a unique identifier ([UUID](https://weaviate.io/developers/weaviate/manage-data/create#id)) for each object in Weaviate. If the object with the same UUID already exists in Weaviate, it will be updated with the new data. Otherwise, a new object will be created.


:::caution

If you are using the `merge` write disposition, you must set it from the first run of your pipeline; otherwise, the data will be duplicated in the database on subsequent loads.

:::

### Append

This is the default disposition. It will append the data to the existing data in the destination, ignoring the `primary_key` field.

## Data loading

Loading data into Weaviate from different sources requires a proper understanding of how data is transformed and integrated into [Weaviate's schema](https://weaviate.io/developers/weaviate/config-refs/schema).

### Data types

Data loaded into Weaviate from various sources might have different types. To ensure compatibility with Weaviate's schema, there's a predefined mapping between the [dlt types](../../general-usage/schema.md#data-types) and [Weaviate's native types](https://weaviate.io/developers/weaviate/config-refs/datatypes):

| dlt Type  | Weaviate Type |
| --------- | ------------- |
| text      | text          |
| double    | number        |
| bool      | boolean       |
| timestamp | date          |
| date      | date          |
| bigint    | int           |
| binary    | blob          |
| decimal   | text          |
| wei       | number        |
| json      | text          |

### Dataset name

Weaviate uses classes to categorize and identify data. To avoid potential naming conflicts, especially when dealing with multiple datasets that might have overlapping table names, dlt includes the dataset name in the Weaviate class name. This ensures a unique identifier for every class.

For example, if you have a dataset named `movies_dataset` and a table named `actors`, the Weaviate class name would be `MoviesDataset_Actors` (the default separator is an underscore).

However, if you prefer to have class names without the dataset prefix, skip the `dataset_name` argument.

For example:

```py
pipeline = dlt.pipeline(
    pipeline_name="movies",
    destination="weaviate",
)
```

### Names normalization

When loading data into Weaviate, dlt tries to maintain naming conventions consistent with the Weaviate schema.

Here's a summary of the naming normalization approach:

#### Table names

- Snake case identifiers such as `snake_case_name` get converted to `SnakeCaseName` (aka Pascal case).
- Pascal case identifiers such as `PascalCaseName` remain unchanged.
- Leading underscores are removed. Hence, `_snake_case_name` becomes `SnakeCaseName`.
- Numbers in names are retained, but if a name starts with a number, it's prefixed with a character, e.g., `1_a_1snake_case_name` to `C1A1snakeCaseName`.
- Double underscores in the middle of names, like `Flat__Space`, result in a single underscore: `Flat_Space`. If these appear at the end, they are followed by an 'x', making `Flat__Space_` into `Flat_Spacex`.
- Special characters and spaces are replaced with underscores, and emojis are simplified. For instance, `Flat Sp!ace` becomes `Flat_SpAce` and `Flat_Sp💡ace` is changed to `Flat_SpAce`.

#### Property names

- Snake case and camel case remain unchanged: `snake_case_name` and `camelCaseName`.
- Names starting with a capital letter have it lowercased: `CamelCase` -> `camelCase`
- Names with multiple underscores, such as `Snake-______c__ase_`, are compacted to `snake_c_asex`. Except for the case when underscores are leading, in which case they are kept: `___snake_case_name` becomes `___snake_case_name`.
- Names starting with a number are prefixed with a "p_". For example, `123snake_case_name` becomes `p_123snake_case_name`.

#### Reserved property names

Reserved property names like `id` or `additional` are prefixed with underscores for differentiation. Therefore, `id` becomes `__id` and `_id` is rendered as `___id`.

### Case insensitive naming convention
The default naming convention described above will preserve the casing of the properties (besides the first letter which is lowercased). This generates nice classes in Weaviate but also requires that your input data does not have clashing property names when comparing case insensitively (i.e., `caseName` == `casename`). In such cases, Weaviate destination will fail to create classes and report a conflict.

You can configure an alternative naming convention which will lowercase all properties. The clashing properties will be merged and the classes created. Still, if you have a document where clashing properties like:
```json
{"camelCase": 1, "CamelCase": 2}
```
it will be normalized to:
```json
{"camelcase": 2}
```
so your best course of action is to clean up the data yourself before loading and use the default naming convention. Nevertheless, you can configure the alternative in `config.toml`:
```toml
[schema]
naming="dlt.destinations.impl.weaviate.ci_naming"
```

## Additional destination options

- `batch_size`: (int) the number of items in the batch insert request. The default is 100.
- `batch_workers`: (int) the maximal number of concurrent threads to run batch import. The default is 1.
- `batch_consistency`: (str) the number of replica nodes in the cluster that must acknowledge a write or read request before it's considered successful. The available consistency levels include:
    - `ONE`: Only one replica node needs to acknowledge.
    - `QUORUM`: Majority of replica nodes (calculated as `replication_factor / 2 + 1`) must acknowledge.
    - `ALL`: All replica nodes in the cluster must send a successful response.
    The default is `ONE`.
- `batch_retries`: (int) number of retries to create a batch that failed with ReadTimeout. The default is 5.
- `dataset_separator`: (str) the separator to use when generating the class names in Weaviate.
- `conn_timeout` and `read_timeout`: (float) to set timeouts (in seconds) when connecting and reading from the REST API. Defaults to (10.0, 180.0).
- `startup_period` (int) - how long to wait for Weaviate to start.
- `vectorizer`: (str) the name of [the vectorizer](https://weaviate.io/developers/weaviate/modules/retriever-vectorizer-modules) to use. The default is `text2vec-openai`.
- `moduleConfig`: (dict) configurations of various Weaviate modules.

### Configure Weaviate modules

The default configuration for the Weaviate destination uses `text2vec-openai`.
To configure another vectorizer or a generative module, replace the default `module_config` value by updating `config.toml`:

```toml
[destination.weaviate]
module_config={text2vec-openai = {}, generative-openai = {}}
```

This ensures the `generative-openai` module is used for generative queries.

### Run Weaviate fully standalone

Below is an example that configures the **contextionary** vectorizer. You can put this into `config.toml`. This configuration does not need external APIs for vectorization and may be used fully offline.
```toml
[destination.weaviate]
vectorizer="text2vec-contextionary"
module_config={text2vec-contextionary = { vectorizeClassName = false, vectorizePropertyName = true}}
```
You can find Docker Compose with the instructions to run [here](https://github.com/dlt-hub/dlt/tree/devel/dlt/destinations/impl/weaviate/README.md).

### dbt support

Currently, Weaviate destination does not support dbt.

### Syncing of `dlt` state

Weaviate destination supports syncing of the `dlt` state.

<!--@@@DLT_TUBA weaviate-->

