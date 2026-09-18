---
title: Weaviate
description: Weaviate is an open source vector database that can be used as a destination in dlt.
keywords: [weaviate, vector database, destination, dlt]
---
# Weaviate

[Weaviate](https://weaviate.io/) is an open-source vector database. It allows you to store data objects and perform similarity searches over them.
This destination helps you load data into Weaviate from [dlt resources](../../general-usage/resource.md).

:::note
The Weaviate destination requires `weaviate-client` 4.20.0 or newer. Server-side batching
additionally requires a Weaviate server on 1.36 or newer.
:::

<!--@@@DLT_DESTINATION_CAPABILITIES weaviate-->

## Setup guide

1. To use Weaviate as a destination, make sure dlt is installed with the 'weaviate' extra:

```sh
pip install "dlt[weaviate]"
```

2. Next, configure the destination in the dlt secrets file. The file is located at `~/.dlt/secrets.toml` by default. Add the following section to the secrets file:

```toml
[destination.weaviate]
connection_type = "cloud"  # or "local" or "custom"

[destination.weaviate.credentials]
url = "https://your-weaviate-url"
api_key = "your-weaviate-api-key"

[destination.weaviate.credentials.additional_headers]
X-OpenAI-Api-Key = "your-openai-api-key"
```

In this setup guide, we are using the [Weaviate Cloud Services](https://console.weaviate.cloud/) to get a Weaviate instance and [OpenAI API](https://platform.openai.com/) for generating embeddings through the [text2vec-openai](https://weaviate.io/developers/weaviate/modules/retriever-vectorizer-modules/text2vec-openai) module.

You can host your own Weaviate instance using Docker Compose, Kubernetes, or embedded. Refer to Weaviate's [How-to: Install](https://weaviate.io/developers/weaviate/installation) or [dlt recipe we use for our tests](#run-weaviate-locally). In that case, you can skip the credentials part altogether:

```toml
[destination.weaviate.credentials.additional_headers]
X-OpenAI-Api-Key = "your-openai-api-key"
```

The `url` will default to **[http://localhost:8080](http://localhost:8080)** and `api_key` is not defined - which are the defaults for the Weaviate container.

### Connection types

The Weaviate destination supports three connection types that are auto-detected from the URL pattern:

- **cloud**: For [Weaviate Cloud Services](https://console.weaviate.cloud/) - URLs containing `.weaviate.cloud`
- **local**: For local Docker instances - URLs with `localhost` or `127.0.0.1`
- **custom**: For self-hosted instances - any other URL (requires explicit port configuration)

You can also explicitly set the connection type in `config.toml`:

```toml
[destination.weaviate]
connection_type = "cloud"  # or "local" or "custom"
```

Or when creating the destination programmatically:

```py
import dlt

pipeline = dlt.pipeline(
    pipeline_name="my_pipeline",
    destination=dlt.destinations.weaviate(connection_type="cloud"),
)
```

For **custom** connection types, you must specify the HTTP and gRPC ports:

```toml
[destination.weaviate]
connection_type = "custom"

[destination.weaviate.credentials]
url = "http://my-weaviate-host"
http_port = 8080
grpc_port = 50051
```

`[destination.weaviate.credentials]` also accepts:

- `grpc_host`: (str) the gRPC host, when it is not the same as the REST host. Only
  `connection_type = "custom"` supports this — the Weaviate client takes a separate gRPC host
  only on `connect_to_custom`, so setting it for `local` or `cloud` raises a configuration error
  instead of being silently ignored.
- `http_secure` / `grpc_secure`: (bool) whether to use TLS. `http_secure` defaults to whether
  `url` is `https`, and `grpc_secure` defaults to `http_secure`.

```toml
[destination.weaviate]
connection_type = "custom"

[destination.weaviate.credentials]
url = "https://rest.example.com"
http_port = 443
grpc_host = "grpc.example.com"
grpc_port = 50051
grpc_secure = true
```

:::note
The v4 Weaviate client needs both REST **and** gRPC. When self-hosting, make sure the gRPC port
(50051 by default) is reachable, not just the REST port.
:::

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

import dlt
from dlt.destinations.adapters import weaviate_adapter

pipeline = dlt.pipeline(
    pipeline_name="movies",
    destination="weaviate",
    dataset_name="MoviesDataset",
)

info = pipeline.run(
    weaviate_adapter(
        movies,
        vectorize="title",
    )
)

print(info)
```

The data is now loaded into Weaviate.

Weaviate destination is different from other [dlt destinations](../destinations/). To use vector search after the data has been loaded, you must specify which fields Weaviate needs to include in the vector index. You do that by wrapping the data (or dlt resource) with the `weaviate_adapter` function.

## Weaviate adapter

The `weaviate_adapter` is a helper function that configures the resource for the Weaviate destination:

```py
from dlt.destinations.adapters import weaviate_adapter

weaviate_adapter(data, vectorize, tokenization, vector, named_vectors)  # ty: ignore[unresolved-reference]
```

It accepts the following arguments:

- `data`: a dlt resource object or a Python data structure (e.g., a list of dictionaries).
- `vectorize`: a name of the field or a list of names that should be vectorized by Weaviate.
- `tokenization`: the dictionary containing the tokenization configuration for a field. The dictionary should have the following structure `{'field_name': 'method'}`. Valid methods are "word", "lowercase", "whitespace", "field". The default is "word". See [Property tokenization](https://weaviate.io/developers/weaviate/config-refs/schema#property-tokenization) in Weaviate documentation for more details.
- `vector`: the name of a field holding a precomputed embedding. See [Bring your own vectors](#bring-your-own-vectors).
- `named_vectors`: a dictionary declaring several independently configured vectors on one collection. See [Named vectors](#named-vectors).

Returns: a [dlt resource](../../general-usage/resource.md) object that you can pass to the `pipeline.run()`.

Example:

```py
from dlt.destinations.adapters import weaviate_adapter

weaviate_adapter(
    resource,
    vectorize=["title", "description"],
    tokenization={"title": "word", "description": "whitespace"},
)
```

When using the `weaviate_adapter`, it's important to apply it directly to resources, not to the whole source. Here's an example:

```py
import dlt
from dlt.sources.sql_database import sql_database
from dlt.destinations.adapters import weaviate_adapter

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
A more comprehensive pipeline would load data from [API](https://dlthub.com/workspace) or use one of dlt's [sources](../verified-sources/).
:::

### Bring your own vectors

When you already compute embeddings yourself, point `vector` at the field holding them. The
field is stored as the object vector instead of as a property, and the collection is created
without a vectorizer:

```py
@dlt.resource(primary_key="doc_id", write_disposition="merge")
def documents():
    yield {"doc_id": 1, "title": "first", "embedding": [0.1, 0.2, 0.3]}

weaviate_adapter(documents(), vector="embedding")  # ty: ignore[unresolved-reference]
```

The vector index stays enabled so the objects remain searchable, and re-running with `merge`
replaces the vector along with the rest of the object.

### Named vectors

A collection can carry several vectors, each built from its own fields and, optionally, its own
vectorizer. Declare them with `named_vectors`:

```py
weaviate_adapter(  # ty: ignore[unresolved-reference]
    articles(),
    named_vectors={
        "title_vec": {"vectorize": ["title"]},
        "body_vec": {"vectorize": ["body"], "vectorizer": "text2vec-weaviate"},
    },
)
```

Each entry becomes a named vector on the collection, queryable with Weaviate's `target_vector`.
A vector without an explicit `vectorizer` uses the destination's `vectorizer` setting.

:::note
Weaviate fixes a collection's vectors when the collection is created and offers no way to
change them afterwards. Declare `named_vectors` before the first load, or load into a new
dataset. The same applies to `vectorize`: a column that gains the hint after the collection
exists is stored and stays queryable, but is not added to the vector, and dlt logs a warning.
:::

## Write disposition

A [write disposition](../../general-usage/incremental-loading.md#choosing-a-write-disposition) defines how the data should be written to the destination. All write dispositions are supported by the Weaviate destination.

### Replace

The [replace](../../general-usage/full-loading.md) disposition replaces the data in the destination with the data from the resource. It deletes all the classes and objects and recreates the schema before loading the data.

In the movie example from the [setup guide](#setup-guide), we can use the `replace` disposition to reload the data every time we run the pipeline:

```py
from dlt.destinations.adapters import weaviate_adapter

movies = [{"id": 1, "title": "Blade Runner", "year": 1982}, ...]
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
from dlt.destinations.adapters import weaviate_adapter

movies = [{"id": 1, "title": "Blade Runner", "year": 1982}, ...]
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

:::warning

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

- `batch_mode`: (str) how objects are sent to Weaviate. The default is `auto`.
- `batch_size`: (int) the number of items in the batch insert request. Applies to `fixed_size` batching. The default is 100.
- `batch_workers`: (int) the number of concurrent requests. Applies to `fixed_size` and `stream` batching. The default is 1.
- `batch_requests_per_minute`: (int) the request budget per minute. Applies to `rate_limit` batching. The default is 600.
- `batch_consistency`: (str) the number of replica nodes in the cluster that must acknowledge a write or read request before it's considered successful. The available consistency levels include:
  - `ONE`: Only one replica node needs to acknowledge.
  - `QUORUM`: Majority of replica nodes (calculated as `replication_factor / 2 + 1`) must acknowledge.
  - `ALL`: All replica nodes in the cluster must send a successful response.
    The default is `ONE`.
- `dataset_separator`: (str) the separator to use when generating the class names in Weaviate.
- `conn_timeout` and `read_timeout`: (float) to set timeouts (in seconds) when connecting and reading. Defaults to (10.0, 180.0).
- `init_timeout`, `query_timeout`, `insert_timeout`, `stream_timeout`: (float) per-operation timeouts. `init_timeout` overrides `conn_timeout`; `query_timeout` and `insert_timeout` override `read_timeout`. `stream_timeout` applies to a server-side batch stream.
- `skip_init_checks`: (bool) skip the client startup handshake. Defaults to `true` for `cloud` and `custom`, `false` for `local`.
- `proxies`: (dict) proxies passed to the client, e.g. `{"https" = "http://proxy:3128"}`.
- `trust_env`: (bool) let the client read proxy settings from the environment. The default is `false`.
- `session_pool_connections`, `session_pool_maxsize`, `session_pool_max_retries`, `session_pool_timeout`: (int) REST connection pool tuning. Left unset, the client's own defaults apply.
- `vectorizer`: (str) the name of [the vectorizer](https://weaviate.io/developers/weaviate/model-providers) to use. The default is `text2vec-openai`. Any vectorizer module supported by the Weaviate Python client can be used; an unknown name raises an error rather than silently loading unvectorized objects.
- `module_config`: (dict) configurations of various Weaviate modules.
- `multi_tenancy`: (bool) create collections as multi-tenant. The default is `false`.
- `tenant`: (str) the tenant to load into. Requires `multi_tenancy`.
- `auto_tenant_creation`: (bool) create the tenant on first write instead of failing. The default is `true`.
- `auto_tenant_activation`: (bool) activate an inactive tenant on any operation against it instead of failing. The default is `true`.
- `collection_config`: (dict) extra arguments passed to `collections.create`. See [Collection configuration](#collection-configuration).

:::note
`batch_retries` and `startup_period` are deprecated and have no effect. The Weaviate v4 client
manages retries and startup internally. Setting them emits a deprecation warning.
:::

### Batching

`batch_mode` selects how dlt sends objects:

| Mode | Behavior |
| --- | --- |
| `auto` (default) | Uses server-side batching when the server supports it, otherwise falls back to `fixed_size`. |
| `stream` | [Server-side batching](https://docs.weaviate.io/weaviate/tutorials/import#option-a-server-side-batching). The server picks the batch size, parallelization, and backpressure. Requires Weaviate **1.36** or newer. |
| `fixed_size` | Client-side batching with an explicit `batch_size` and `batch_workers`. |
| `rate_limit` | Client-side batching capped at `batch_requests_per_minute`, for rate-limited vectorizer APIs. |
| `dynamic` | Client-side batching where the client adjusts the batch size. |

Server-side batching needs no tuning and is the recommended mode, so `auto` picks it whenever
the connected server is new enough:

```toml
[destination.weaviate]
batch_mode = "auto"
```

If your vectorizer provider rate-limits you, cap the request rate instead:

```toml
[destination.weaviate]
batch_mode = "rate_limit"
batch_requests_per_minute = 100
```

### Collection configuration

Most collection settings are reachable through `module_config`, which is passed straight to the
vectorizer factory. That covers the vector index and quantization:

```py
from weaviate.classes.config import Configure
import dlt

destination = dlt.destinations.weaviate(  # ty: ignore[unresolved-attribute]
    vectorizer="text2vec-cohere",
    module_config={
        "text2vec-cohere": {
            "model": "embed-multilingual-v3.0",
            "vector_index_config": Configure.VectorIndex.hnsw(
                ef_construction=256,
                max_connections=64,
                quantizer=Configure.VectorIndex.Quantizer.bq(),
            ),
        }
    },
)
```

Everything else Weaviate accepts when creating a collection — `replication_config`,
`sharding_config`, `inverted_index_config`, `generative_config`, `reranker_config`,
`object_ttl_config`, `description`, `references` — goes through `collection_config`, which dlt
merges into every collection it creates:

```py
from weaviate.classes.config import Configure
import dlt

destination = dlt.destinations.weaviate(  # ty: ignore[unresolved-attribute]
    collection_config={
        "description": "loaded by dlt",
        "replication_config": Configure.replication(factor=3),
        "generative_config": Configure.Generative.cohere(),
    },
)
```

`name`, `properties`, `vector_config` and `multi_tenancy_config` are derived by dlt from the
schema and cannot be set here — passing one raises, rather than silently producing a collection
that does not match the schema. An argument Weaviate does not accept raises too, listing the ones
it does.

Because the values are weaviate-client config objects, `collection_config` and the richer parts of
`module_config` can only be set from Python, not from `secrets.toml`.

### Weaviate integration header

dlt identifies itself to Weaviate with the `X-Weaviate-Client-Integration` header, set to
`dlt/<version>`. It is sent on every connection so Weaviate can attribute traffic to the
integration. To change or remove it, set the same header explicitly:

```toml
[destination.weaviate.credentials.additional_headers]
X-Weaviate-Client-Integration = "my-app/1.0"
```

### Multi-tenancy

Weaviate [multi-tenant collections](https://docs.weaviate.io/weaviate/manage-collections/multi-tenancy)
keep each tenant's objects isolated. Enable it and name the tenant this pipeline loads into:

```toml
[destination.weaviate]
multi_tenancy = true
tenant = "my-tenant"
```

Tenants are created on demand, and an inactive tenant is activated by any operation against it.
Without `auto_tenant_activation` a load into such a tenant fails with `tenant not active`, so dlt
turns it on by default; Weaviate is making it the default too.

dlt's own `_dlt_*` collections stay single-tenant: they hold pipeline bookkeeping, which is
already keyed by pipeline name, not tenant data.

### Configure Weaviate modules

The default configuration for the Weaviate destination uses `text2vec-openai`.
To configure another vectorizer or a generative module, replace the default `module_config` value by updating `config.toml`:

```toml
[destination.weaviate]
module_config={text2vec-openai = {}, generative-openai = {}}
```

This ensures the `generative-openai` module is used for generative queries.

### Run Weaviate locally

You can run Weaviate locally using Docker. See the [Weaviate Local Quickstart](https://weaviate.io/developers/weaviate/quickstart/local) for details.

Below is an example that configures the **contextionary** vectorizer in `config.toml`. This does not require external APIs and can run fully offline:

```toml
[destination.weaviate]
connection_type = "local"
vectorizer = "text2vec-contextionary"
module_config = {text2vec-contextionary = {vectorizeClassName = false, vectorizePropertyName = true}}
```

You can find the Docker Compose file and setup instructions in our [README](https://github.com/dlt-hub/dlt/tree/devel/dlt/destinations/impl/weaviate/README.md).

### dbt support

Currently, Weaviate destination does not support dbt.

### Syncing of `dlt` state

Weaviate destination supports syncing of the `dlt` state.

<!--@@@DLT_TUBA weaviate-->
