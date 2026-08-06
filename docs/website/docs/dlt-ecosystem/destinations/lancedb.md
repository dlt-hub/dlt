---
title: 🧪 LanceDB Enterprise
description: LanceDB is a multimodal lakehouse for AI that can be used as a destination in dlt.
keywords: [ lancedb, vector database, destination, dlt ]
---

# 🧪 LanceDB Enterprise

[LanceDB](https://lancedb.com/) is a multimodal lakehouse for AI, built on top of [Lance](https://lance.org), an open-source lakehouse format. You can store data objects in it and search them by similarity.
This destination helps you load data into LanceDB from [dlt resources](../../general-usage/resource.md).

This destination connects to a [**LanceDB Enterprise cluster**](https://docs.lancedb.com/enterprise/index) or LanceDB Cloud. The cluster does all
storage IO, so `dlt` needs no object store credentials. To load into a self-managed Lance lakehouse
of your own — a directory or REST catalog over your own bucket — use the
[`lance` destination](./lance.md) instead.


<!--@@@DLT_DESTINATION_CAPABILITIES lancedb-->

## Setup guide

### Choose a model provider

First, you need to decide which embedding model provider to use. You can find all supported providers by visiting the official [LanceDB docs](https://docs.lancedb.com/embedding/index#embedding-model-providers).

### Install dlt with LanceDB

To use LanceDB as a destination, make sure `dlt` is installed with the `lancedb` extra:

```sh
pip install "dlt[lancedb]"
```

The lancedb extra installs only `dlt` and `lancedb`. Install your model provider's SDK as well.

You can find which libraries you need by also referring to the [LanceDB docs](https://docs.lancedb.com/embedding/index#embedding-model-providers).

### Configure the destination

Configure the destination in the dlt secrets file located at `~/.dlt/secrets.toml` by default. Add the following section:

```toml
[destination.lancedb.credentials]
api_key = "api_key"
database = "my_database"  # optional, sets one database as the dataset, see below
host_override = "https://my-cluster.example.com"  # required for Enterprise, omit for LanceDB Cloud
region = "us-east-1"  # region of a LanceDB Cloud database
flightsql_host = "my-flight-endpoint.example.com"  # enables SQL reads, see below
read_consistency_interval_seconds = 0  # how stale a managed client read can be

[destination.lancedb.embeddings]
provider = "ollama"
name = "mxbai-embed-large"
kwargs = { host = "http://localhost:11434" }  # provider specific arguments, for example a custom endpoint

[destination.lancedb.embeddings.credentials]
api_key = "embedding_model_provider_api_key"  # not needed for providers without authentication (ollama, sentence-transformers)
```

- The `api_key` authenticates to the cluster. It is required.
- The `host_override` is the endpoint of an Enterprise cluster. Leave it out for LanceDB Cloud, which `region` identifies.
- The `database` is optional. Leave it out and every [dataset becomes a database](#datasets-are-databases). Set it to [configure one database](#configure-one-database) for the destination.
- The `flightsql_host` is the Arrow Flight SQL endpoint used for reading. Enterprise serves it from a
  separate load balancer, on port `10025` by default (`flightsql_port`, `flightsql_tls`). Without it,
  loading works but [reading](#access-loaded-data) is disabled.
- The `read_consistency_interval_seconds` asks the managed client for reads no staler than the given number of seconds. See [read freshness](#read-freshness).

The `embeddings` section is shared with the [lance](lance.md) destination and is optional: leave it
out and no vector column is added.

- The `provider` generates the embeddings, for example `cohere` or `openai`.
- The `name` is the provider's model, for example `embed-english-v3.0`.
  Reference https://lancedb.github.io/lancedb/embeddings/default_embedding_functions/.
- The `vector_column` names the column holding the embeddings. Defaults to `vector`.
- The `dimensions` sets the embedding dimensionality. Inferred from the model when not set.
- The `max_retries` bounds retries of embedding requests, `3` by default. Set it to `0` to disable them.
- The `kwargs` are passed to the provider's embedding function, which is how providers with custom
  endpoints (like Ollama) receive their `host`.
- The `credentials.api_key` authenticates to the embedding provider. Providers that need no
  authentication, such as Ollama, do not need it.

A row whose embedded column is empty or `NULL` has nothing to embed, so it lands with a `NULL`
vector rather than failing the load. The row and its other columns are preserved.

### Datasets are databases

The `dataset_name` of a pipeline is a **database** of the cluster, and all tables of the dataset,
including the `dlt` tables, live in that database's root namespace. This is what lets the SQL
endpoint read them, as it addresses a table as `"<dataset>"."public"."<table>"`, and it is why
[joins across datasets](#join-across-datasets) are possible.

```py
import dlt

# tables land in the `analytics` database
pipeline = dlt.pipeline("movies", destination="lancedb", dataset_name="analytics")
```

A database is created on the first load. Dataset names are normalized like any other identifier, so
`My-Analytics` becomes the database `my_analytics`.

`dlt` also creates an empty namespace named `_dlt_sentinel` in the database. A database that holds no
tables cannot be told apart from one that was never created, so this namespace is what records that
the dataset exists. `drop_storage` removes the tables and then the sentinel. The emptied database is
indistinguishable from one that never existed, and it holds nothing.

#### Configure one database

Setting `credentials.database` gives the destination a single database, which then **is** the
dataset. This loads into a database whose name is not a valid dataset name, because a configured name
skips normalization. The dataset must name that same database, otherwise the load is refused rather
than writing somewhere you did not ask for:

```py
import dlt

# `dlt-ci-5` normalizes to `dlt_ci_5`, so configure it and name the dataset after it
pipeline = dlt.pipeline(
    "movies",
    destination=dlt.destinations.lancedb(credentials={"database": "dlt-ci-5", "api_key": "..."}),
    dataset_name="dlt-ci-5",
)
```

The configured database can hold tables of a foreign dataset, so `drop_storage` removes only the
destination tables of the current schema there and warns about what it skipped.

Passing an already connected client configures its database the same way:

```py
import lancedb
import dlt

db = lancedb.connect("db://my_database", api_key="...", host_override="https://my-cluster.example.com")
pipeline = dlt.pipeline(
    "movies", destination=dlt.destinations.lancedb(credentials=db), dataset_name="my_database"
)
```

### Join across datasets

Each dataset is its own database and therefore its own SQL catalog, so a join across two datasets is
plain SQL:

```py
import dlt

characters = dlt.pipeline("characters", destination="lancedb", dataset_name="characters")
quests = dlt.pipeline("quests", destination="lancedb", dataset_name="quests")

joined = (
    characters.dataset()
    .table("characters")
    .join(quests.dataset().table("quests"), on="characters.id = quests.character_id")
)
print(joined.df())
```

### Name a load with `commit_tag`

Set `commit_tag` to name the version each table has at the end of a load:

```toml
[destination.lancedb]
commit_tag = "nightly"
```

Every table `dlt` owns gets the tag, including the `dlt` tables and tables that received no data in
that load, so the tag names the **whole dataset** as it stood when the load finished. Tables of a
foreign dataset in the same database are never tagged, which matters when you
[configure one database](#configure-one-database) that a foreign dataset shares.

Loading again under the same name moves the tag forward, so a fixed name like `nightly` is a rolling
pointer to the last completed load. Use a fresh name per load only when you intend to keep every one
of them — see the retention note below.

A tag does two useful things.

#### It retains a version against cleanup

An Enterprise cluster compacts and prunes in the background: `optimize()` is a no-op there, and old
versions are eventually removed. **A tagged version is exempt** — it is retained regardless of age
until the tag is deleted. Tagging is therefore the only way to keep a past load readable, and the
reason a unique tag per load accumulates versions that can never be pruned.

#### It is a rollback target

```py
import dlt
from dlt.destinations.impl.lancedb.lancedb_adapter import rollback_to_commit_tag

pipeline = dlt.pipeline("movies", destination="lancedb", dataset_name="analytics")
rollback_to_commit_tag(pipeline.dataset(), "nightly")
pipeline.run(corrected_data)  # continues from the restored state
```

A rollback appends a new version holding the tagged contents rather than deleting anything, so history
survives and the rollback itself can be undone by rolling back to a later tag. `rollback_to_commit_tag`
returns the tables it restored, and waits for the cluster to publish each restore, because a load
started too early fails.

:::caution
LanceDB has no transaction spanning tables, so a rollback is applied table by table. If it fails part
way the dataset mixes versions. The tables already restored are logged, and running it again is safe.
:::

To read a tagged version without rolling back, check it out through the managed client:

```py
with pipeline.destination_client() as client:
    table = client.open_table("movies")  # type: ignore[attr-defined]
    table.checkout("nightly")
    print(table.count_rows())
```

The Arrow Flight SQL endpoint has no time-travel syntax, so `dataset()` always reads the current
version. Only the managed client or a rollback can access a tag.

Data tables are tagged before the load is committed, so a tagging failure aborts the load and `dlt`
retries it. The `_dlt_loads` table is tagged immediately after the row that marks the load complete,
and that one cannot be retried: if it fails, the error names the tag and the exact version so you can
create it by hand.

### Load vectorized documents with optional chunking

A document is a row, and the column you name in `lancedb_adapter(embed=...)` is the one LanceDB
embeds so you can search it by similarity. Start with one row per document:

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

#### Create a pipeline

```py
pipeline = dlt.pipeline(
  pipeline_name="movies",
  destination="lancedb",
)
```

#### Run the pipeline

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

To use **vector search** after loading, you **must specify which fields LanceDB generates embeddings for**. Do this by wrapping the data (or dlt resource) with the **`lancedb_adapter`** function. Above we requested the embedding to be created on `title` column using the configured embedding provider and model.

:::note
The `movies` table lives in the root namespace of the [database named after the
dataset](#datasets-are-databases), which is how the SQL endpoint accesses it.
:::

#### Chunk long documents

A document too long to embed as a single vector is split into chunks, and each chunk becomes a row.
Document identity and chunk identity are then two different things, and the resource must declare
both:

```py
@dlt.resource(
  primary_key=["doc_id", "chunk_hash"],
  merge_key="doc_id",
  write_disposition={"disposition": "merge", "strategy": "upsert"},
)
def rag_docs(documents):
  for document in documents:
    for chunk in chunk_text(document["body"]):
      yield {
        "doc_id": document["id"],
        "chunk_hash": digest128(chunk),
        "chunk": chunk,
      }
```

- `primary_key` identifies a **chunk**, so a chunk whose text changes becomes a different row.
- `merge_key` identifies the **document**. It cannot be compound and must be the first element of
  `primary_key`.

Embed the chunk column rather than the whole document:

```py
pipeline.run(lancedb_adapter(rag_docs(documents), embed="chunk"))
```

#### Remove orphaned chunks when a document is reloaded

Reloading a document usually means its text changed: some chunks survive, some are new, and some no
longer exist. An upsert writes the new chunks and updates the surviving ones, but the chunks that
disappeared stay in the table — so a similarity search keeps returning text the document no longer
contains. **Orphan removal deletes them.** This is its main job, and it works on the root table of
chunks, which is why the `merge_key` matters: deletion is scoped to the documents this load carries,
so chunks of every other document are left alone.

It does the same one level down, removing records of a nested table whose parent record is gone.

A single `merge_key` names the document, so **orphan removal is on whenever a resource defines one**
— `rag_docs` above needs nothing further. Without a merge key it stays off. Pass `remove_orphans` to
override that:

```py
pipeline.run(
  lancedb_adapter(
    rag_docs(documents),
    embed="chunk",
    remove_orphans=False
  )
)
```

`remove_orphans=True` also turns it on for a resource with no merge key, where the first element of
the `primary_key` becomes the document id. A compound merge key is rejected, since the deletion
filter takes one column.

The resource above already carries the keys and the write disposition, so they do not need repeating
here. On a resource that does not, pass `merge_key` to `lancedb_adapter` and `primary_key` and
`write_disposition` to `run`.

:::note
Orphan removal matches rows on `_dlt_id`, which arrow sources generate randomly per load, so a row of an earlier load is never matched. Give such a resource a `primary_key` so the row key is derived from it and stays stable across loads.
:::

## Use an adapter to specify columns to vectorize

By default, LanceDB acts as a normal database. To use its embedding functions, specify which fields to embed in your dlt resource.

The `lancedb_adapter` is a helper function that configures the resource for the LanceDB destination:

```py
lancedb_adapter(data, embed="title")
```

It accepts the following arguments:

- `data`: a dlt resource object, or a Python data structure (for example, a list of dictionaries).
- `embed`: a name of the field or a list of names to generate embeddings for.

Returns: [dlt resource](../../general-usage/resource.md) object that you can pass to the `pipeline.run()`.

Example:

```py
lancedb_adapter(
  resource,
  embed=["title", "description"],
)
```

Apply the `lancedb_adapter` directly to resources, not to the whole source. Here is an example:

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
Both `dlt` and `LanceDB` support Arrow and Pandas natively. You can [ingest data with high performance](../verified-sources/arrow-pandas.md) without unnecessary rewrites and copies.

If you plan to use `merge` write disposition, remember to [enable load ids](../verified-sources/) tracking for arrow tables.


## Access loaded data

Reads go through the cluster's **Arrow Flight SQL** endpoint, so they run server side and need no
object store credentials. Configure `flightsql_host` and use the regular
[dataset interface](../../general-usage/dataset-access/dataset.md):

```py
dataset = pipeline.dataset()

print(dataset.table("movies").df())
print(dataset("select title from movies limit 5").arrow())
```

Results are served as Arrow, so `arrow()` and `iter_arrow(chunk_size=...)` stream without a row by row
round trip.

### Ibis backend

`ibis` has no LanceDB backend, so `dataset.ibis()` returns the `dlt` backend instead. It compiles an
ibis expression to SQL and lets the Flight SQL endpoint run it, which needs `flightsql_host` and the
`ibis-framework` package:

```py
backend = pipeline.dataset().ibis()

print(backend.list_tables())
print(backend.table("movies").select("title").limit(5).to_pandas())
```

A single relation converts on its own, which keeps the rest of the query in `dlt`:

```py
items = pipeline.dataset().table("movies").to_ibis()
```

`list_tables()` covers every schema of the dataset, not only the default one. The `read_only` flag of
`dataset.ibis()` is accepted and ignored, because the endpoint reads only.

### Read freshness

A cluster serves reads no staler than its own `read_consistency_interval_seconds`, and `dlt`
exposes a credential of the same name that asks a connection for a tighter bound:

```toml
[destination.lancedb.credentials]
read_consistency_interval_seconds = 10  # managed client reads can lag by up to 10s
```

Loading always reads the latest version regardless of this setting, so a merge never matches against
stale rows.

:::caution
On the Enterprise cluster we measured, this setting made **no difference at all** - that includes 0 (default) value. 
:::

### Vector search in SQL

The endpoint is a DataFusion engine, so nearest neighbour search is a plain query using
`array_distance` (L2), `cosine_distance` or `dot_product`. The query vector must be cast to the
column type, otherwise it is compared as a list of `float64`:

```py
query_vector = [0.2, 0.9, 0.4, 0.9]
vector_literal = f"arrow_cast({query_vector}, 'FixedSizeList(4, Float32)')"
table_name = dataset.sql_client.make_qualified_table_name("movies")

nearest = dataset(
    f"select title, array_distance(vector, {vector_literal}) as distance"
    f" from {table_name} order by distance limit 5",
    _execute_raw_query=True,
).arrow()
```

:::note
SQL vector search is a brute force scan: it does **not** use the ANN index. For indexed search use
`search()` on the managed client, which is the index accelerated path.
:::

To use the managed client directly — for indexed search, tags or index management — take it from the
pipeline:

```py
with pipeline.destination_client() as job_client:
    tbl = job_client.open_table("movies")  # type: ignore[attr-defined]
    print(tbl.search("magic dog", query_type="vector").select(["title"]).to_list())
```

## Bring your own vectors
When `embeddings` is configured, `dlt` adds a vector column using the fields marked in `lancedb_adapter`. You can also pass vector data explicitly. Currently this function is available only if
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

The [merge](../../general-usage/incremental-loading.md) write disposition merges the data from the resource with the data at the destination based on a unique identifier. The LanceDB destination supports `upsert` and `insert-only` merge strategies. `upsert` updates existing records and inserts new ones. `insert-only` inserts new records without updating existing ones (see [insert-only strategy](../../general-usage/merge-loading.md#insert-only-strategy)).

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
The `merge_key`, which cannot be compound, must correspond to the canonical `doc_id` used in vector databases and represent the document identifier in your data model.
It must be the first element of the `primary_key`.
This `merge_key` is crucial for document identification and orphan removal during merge operations.
This structure ensures proper record identification and maintains consistency with vector database concepts.


While it's possible to omit the `merge_key` for brevity (in which case it is assumed to be the first entry of `primary_key`),
explicitly specifying both is recommended for clarity.

With a `merge_key` set, a merge also deletes the chunks a reloaded document no longer produces — see
[remove orphaned chunks when a document is reloaded](#remove-orphaned-chunks-when-a-document-is-reloaded).

### Append

This is the default disposition. It will append the data to the existing data in the destination.

## Additional destination options

- `commit_tag`: Names the version every table has at the end of a load, which retains it against cleanup and gives a rollback target. See [name a load](#name-a-load-with-commit_tag).
- `embeddings`: Embedding provider, model and credentials. See [configure the destination](#configure-the-destination).

## Current limitations (LanceDB Enterprise)

- **A merge or a column addition does not advance the table version.** The cluster commits both
  without moving the current version, so a reader that caches keeps serving the previous version for
  about 20 seconds. `dlt` publishes the commit at once with a delete that matches no rows, because a
  load reads its own writes back immediately. That costs one extra version per merge and per column
  addition. An append, a replace and a delete advance the version themselves, and no data is lost
  either way.
- **`read_consistency_interval_seconds` has no effect.** The cluster serves reads at its own
  staleness, so treat the setting as a request it can ignore. See [read freshness](#read-freshness).
- **A filtered query that selects no column of the table panics.** The simplest case is
  `SELECT COUNT(*) FROM t WHERE id = 1`, which returns `must either specify a row count or at least
  one column`.
- **Version and metadata changes have no transaction.** `dlt` cannot tag a table and advance its
  version in one commit, and a load package is not atomic across its destination tables.
- **SQL reads resolve the root namespace of a database only.** This is why a dataset is a database
  rather than a namespace: the endpoint cannot access a table in a child namespace under any
  spelling, so `dlt` never creates one.
- **Adding a column goes through SQL, not arrow.** The cluster rejects arrow schemas when altering a
  table, so `dlt` carries the arrow type in an `arrow_cast` expression. A column whose arrow type has
  no DataFusion name, such as a struct, cannot be added to an existing table.
- **No branches.** The managed client cannot select a branch, so a `commit_tag` takes their place.
  Unlike the `lance` destination there is no write isolation.
- **Flight SQL is query only.** It has no prepared statements, no transactions and no catalog
  metadata queries (`SHOW TABLES`, `information_schema`).

## dbt support

The LanceDB destination does not support dbt integration.

## Syncing of `dlt` state

The LanceDB destination supports syncing of the `dlt` state.

<!--@@@DLT_TUBA lancedb-->
