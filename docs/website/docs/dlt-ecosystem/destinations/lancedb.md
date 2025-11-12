---
title: LanceDB
description: LanceDB is an open source vector database that can be used as a destination in dlt.
keywords: [ lancedb, vector database, destination, dlt ]
---

# LanceDB

[LanceDB](https://lancedb.com/) is an open-source, high-performance vector database. It allows you to store data objects and perform similarity searches over them.
This destination helps you load data into LanceDB from [dlt resources](../../general-usage/resource.md).


<!--@@@DLT_DESTINATION_CAPABILITIES lancedb-->

## Setup guide

### Choose a model provider

First, you need to decide which embedding model provider to use. You can find all supported providers by visiting the official [LanceDB docs](https://lancedb.github.io/lancedb/embeddings/default_embedding_functions/).

### Install dlt with LanceDB

To use LanceDB as a destination, make sure `dlt` is installed with the `lancedb` extra:

```sh
pip install "dlt[lancedb]"
```

The lancedb extra only installs `dlt` and `lancedb`. You will need to install your model provider's SDK.

You can find which libraries you need by also referring to the [LanceDB docs](https://lancedb.github.io/lancedb/embeddings/default_embedding_functions/).

### Configure the destination

Configure the destination in the dlt secrets file located at `~/.dlt/secrets.toml` by default. Add the following section:

```toml
[destination.lancedb]
lance_uri = ".lancedb"
embedding_model_provider = "ollama"
embedding_model = "mxbai-embed-large"
embedding_model_provider_host = "http://localhost:11434"  # Optional: custom endpoint for providers that support it

[destination.lancedb.credentials]
api_key = "api_key" # API key to connect to LanceDB Cloud. Comment out if you are using LanceDB OSS.
embedding_model_provider_api_key = "embedding_model_provider_api_key" # Not needed for providers that don't need authentication (ollama, sentence-transformers).
```

- The `lance_uri` specifies the location of your LanceDB instance. It defaults to a local, on-disk instance if not provided.
- The `api_key` is your API key for LanceDB Cloud connections. If you're using LanceDB OSS, you don't need to supply this key.
- The `embedding_model_provider` specifies the embedding provider used for generating embeddings. The default is `cohere`.
- The `embedding_model` specifies the model used by the embedding provider for generating embeddings.
  Check with the embedding provider which options are available.
  Reference https://lancedb.github.io/lancedb/embeddings/default_embedding_functions/.
- The `embedding_model_provider_host` specifies the full host URL with protocol and port for providers that support custom endpoints (like Ollama). If not specified, the provider's default endpoint will be used.
- The `embedding_model_provider_api_key` is the API key for the embedding model provider used to generate embeddings. If you're using a provider that doesn't need authentication, such as Ollama, you don't need to supply this key.

:::info Available model providers
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
- "ollama"
:::

:::info
Local database name and location:

`lancedb` databases follow the same naming rules as `duckdb`:
1. By default, the database file name is `<pipeline_name>.lancedb` and is placed in current working directory.
2. For a named destination, database file name is `<destination name>.lancedb`
3. The `:pipeline:` `lance_uri` will place database file in pipeline working folder
:::

### Configure cloud destination
`lance_uri` starting with **db://** schema is interpreted as location on LandeDB cloud. In that case you need to pass `api_key` in order to connect. `dlt` uses [the same names as LanceDB connect() function](https://lancedb.github.io/lancedb/python/python/#connections-synchronous):

```toml
[destination.lancedb.credentials]
api_key = "api_key"
region = "us-east-1"
read_consistency_interval=2.5
```
`read_consistency_interval` is None by default (no read consistency, `dlt` assumes that it is a single writer to particular table.)

:::tip
You can pass `storage_options` in the credentials collection that allows to [store lancedb data on a bucket](https://lancedb.github.io/lancedb/guides/storage/). You can try this out but we didn't test that.
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
)
```

### Run the pipeline:

```py
info = pipeline.run(
  lancedb_adapter(
    movies,
    embed="title",
  ),
  table_name="movies",
)
```

The data is now loaded into LanceDB.

To use **vector search** after loading, you **must specify which fields LanceDB should generate embeddings for**. Do this by wrapping the data (or dlt resource) with the **`lancedb_adapter`** function. Above we requested the embedding to be created on `title` column using the configured embedding provider and model.

:::note
We created `pipeline` without a dataset name. In the example above data is stored in `movies` table as expected. If dataset name is specified, `dlt` follows
the same pattern as for other schema-less storages: it will prefix all the tables with `database_name`. For example:
```py
pipeline = dlt.pipeline(
  pipeline_name="movies",
  destination="lancedb",
  dataset_name="movies_db",
)
```
will name the table `movies_db___movies` where `___` (3 underscores) is a configurable separator.

:::

## Use an adapter to specify columns to vectorize

Out of the box, LanceDB will act as a normal database. To use LanceDB's embedding facilities, you'll need to specify which fields you'd like to embed in your dlt resource.

The `lancedb_adapter` is a helper function that configures the resource for the LanceDB destination:

```py
lancedb_adapter(data, embed="title")
```

It accepts the following arguments:

- `data`: a dlt resource object, or a Python data structure (e.g., a list of dictionaries).
- `embed`: a name of the field or a list of names to generate embeddings for.

Returns: [dlt resource](../../general-usage/resource.md) object that you can pass to the `pipeline.run()`.

Example:

```py
lancedb_adapter(
  resource,
  embed=["title", "description"],
)
```

When using the `lancedb_adapter`, it's important to apply it directly to resources, not to the whole source. Here's an example:

```py
products_tables = sql_database().with_resources("products", "customers")

pipeline = dlt.pipeline(
        pipeline_name="postgres_to_lancedb_pipeline",
        destination="lancedb",
    )

# Apply adapter to the needed resources
lancedb_adapter(products_tables.products, embed="description")
lancedb_adapter(products_tables.customers, embed="bio")

info = pipeline.run(products_tables)
```

## Load data with Arrow or Pandas
Both `dlt` and `LanceDB` support Arrow and Pandas natively. You will be able to [ingest data with high performance](../verified-sources/arrow-pandas.md) and without unnecessary rewrites and copies.

If you plan to use `merge` write disposition, remember to [enable load ids](../verified-sources/) tracking for arrow tables.


## Access loaded data

You can access the data that got loaded in many ways. You can create lancedb client yourself, pass it to `dlt` pipeline
for loading and then use it for querying:
```py
import dlt
import lancedb

db = lancedb.connect("movies.db")

pipeline = dlt.pipeline(
  pipeline_name="movies",
  destination=dlt.destinations.lancedb(credentials=db),
)

...

tbl = db.open_table("movies")
print(tbl.query("magic dog"))
```

Alternatively you can get authenticated client from the pipeline:
```py
import dlt
from lancedb import DBConnection

pipeline = dlt.pipeline(
  pipeline_name="movies",
  destination="lancedb",
)

...

with pipeline.destination_client() as job_client:
  db: DBConnection = job_client.db_client  # type: ignore
  tbl = db.open_table("movies")
  tbl.create_scalar_index("id")

```

## Bring your own vectors
By default `dlt` will add a vector column automatically using the embeddings indicated in `lancedb_adapter`. You can also choose to pass vector data explicitly. Currently this function is available only if
you yield Arrow tables with properly created schema. Remember to declare your vector as fixed length:

```py
import pyarrow as pa
import numpy as np
import dlt

vector_dim = 5
vectors = [np.random.rand(vector_dim).tolist() for _ in range(4)]
table = pa.table(
    {
        "id": pa.array(list(range(1, 5)), pa.int32()),
        "vector": pa.array(
            vectors, pa.list_(pa.float32(), vector_dim)
        ),
    }
)

print(dlt.run(table, table_name="vectors", destination="lancedb"))
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

The [merge](../../general-usage/incremental-loading.md) write disposition merges the data from the resource with the data at the destination based on a unique identifier. The LanceDB destination merge write disposition only supports upsert strategy. This updates existing records and inserts new ones based on a unique identifier.

You can specify the merge disposition, primary key, and merge key either in a resource or adapter:

```py
@dlt.resource(
  primary_key=["doc_id", "chunk_id"],
  merge_key=["doc_id"],
  write_disposition={"disposition": "merge", "strategy": "upsert"},
)
def my_rag_docs(
  data: List[DictStrAny],
) -> Generator[List[DictStrAny], None, None]:
    yield data
```

Or:

```py
pipeline.run(
  lancedb_adapter(
    my_new_rag_docs,
    merge_key="doc_id"
  ),
  write_disposition={"disposition": "merge", "strategy": "upsert"},
  primary_key=["doc_id", "chunk_id"],
)
```

The `primary_key` uniquely identifies each record, typically comprising a document ID and a chunk ID.
The `merge_key`, which cannot be compound, should correspond to the canonical `doc_id` used in vector databases and represent the document identifier in your data model.
It must be the first element of the `primary_key`.
This `merge_key` is crucial for document identification and orphan removal during merge operations.
This structure ensures proper record identification and maintains consistency with vector database concepts.


#### Orphan Removal

LanceDB **automatically removes orphaned chunks** when updating or deleting parent documents during a merge operation. To disable this feature:

```py
pipeline.run(
  lancedb_adapter(
    movies,
    embed="title",
    no_remove_orphans=True # Disable with the `no_remove_orphans` flag.
  ),
  write_disposition={"disposition": "merge", "strategy": "upsert"},
  primary_key=["doc_id", "chunk_id"],
)
```

While it's possible to omit the `merge_key` for brevity (in which case it is assumed to be the first entry of `primary_key`),
explicitly specifying both is recommended for clarity.

:::note
Orphan removal requires the presence of the `_dlt_id` and `_dlt_load_id` fields, which are not included by default when arrow tables are loaded. You must [enable it](../../dlt-ecosystem/verified-sources/arrow-pandas#add-_dlt_load_id-and-_dlt_id-to-your-tables) by setting the `add_dlt_id` option to `true` in the normalize configuration.
:::

### Append

This is the default disposition. It will append the data to the existing data in the destination.

## Additional destination options

- `dataset_separator`: The character used to separate the dataset name from table names. Defaults to "___".
- `vector_field_name`: The name of the special field to store vector embeddings. Defaults to "vector".
- `max_retries`: The maximum number of retries for embedding operations. Set to 0 to disable retries. Defaults to 3.

## dbt support

The LanceDB destination doesn't support dbt integration.

## Syncing of `dlt` state

The LanceDB destination supports syncing of the `dlt` state.

<!--@@@DLT_TUBA lancedb-->

