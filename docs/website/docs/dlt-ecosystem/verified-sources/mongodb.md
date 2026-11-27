---
title: MongoDB
description: dlt verified source for MongoDB
keywords: [mongodb, verified source, mongo database]
---
# MongoDB

import Header from './_source-info-header.md';

<Header/>

[MongoDB](https://www.mongodb.com/what-is-mongodb) is a NoSQL database that stores JSON-like
documents.

This MongoDB `dlt` verified source and
[pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/mongodb_pipeline.py)
loads data using the "MongoDB" source to the destination of your choice.

Sources and resources that can be loaded using this verified source are:

| Name               | Description                                |
| ------------------ | ------------------------------------------ |
| mongodb            | Loads a specific MongoDB database          |
| mongodb_collection | Loads a collection from a MongoDB database |

## Setup guide

### Grab credentials

#### Grab `connection_url`

MongoDB can be configured in multiple ways. Typically, the connection URL format is:

```text
connection_url = "mongodb://dbuser:passwd@host.or.ip:27017"
```

For details on connecting to MongoDB and obtaining the connection URL, see
[the documentation.](https://www.mongodb.com/docs/drivers/go/current/fundamentals/connection/)

Here are the typical ways to configure MongoDB and their connection URLs:

| Name                | Description                                                                           | Connection URL Example                            |
| ------------------- | ------------------------------------------------------------------------------------- | ------------------------------------------------- |
| Local installation  | Install on Windows, macOS, Linux using official packages.                             | "mongodb://dbuser:passwd@host.or.ip:27017"        |
| Docker              | Deploy using the MongoDB Docker image.                                                | "mongodb://dbuser:passwd@docker.host:27017"       |
| MongoDB Atlas       | MongoDB’s managed service on AWS, Azure, and Google Cloud.                            | "mongodb+srv://dbuser:passwd@cluster.mongodb.net" |
| Managed cloud       | AWS DocumentDB, Azure Cosmos DB, and others offer MongoDB as a managed database.      | "mongodb://dbuser:passwd@managed.cloud:27017"     |
| Configuration tools | Use Ansible, Chef, or Puppet for automation of setup and configuration.               | "mongodb://dbuser:passwd@config.tool:27017"       |
| Replica set         | Set up for high availability with data replication across multiple MongoDB instances. | "mongodb://dbuser:passwd@replica.set:27017"       |
| Sharded cluster     | Scalable distribution of datasets across multiple MongoDB instances.                  | "mongodb://dbuser:passwd@shard.cluster:27017"     |
| Kubernetes          | Deploy on Kubernetes using Helm charts or operators.                                  | "mongodb://dbuser:passwd@k8s.cluster:27017"       |
| Manual tarball      | Install directly from the official MongoDB tarball, typically on Linux.               | "mongodb://dbuser:passwd@tarball.host:27017"      |

> Note: The provided URLs are example formats; adjust as needed for your specific setup.

#### Grab `database and collections`

1. To grab "database and collections," you must have the MongoDB shell installed. For installation
   guidance, refer to [the documentation here.](https://www.mongodb.com/docs/mongodb-shell/install/)

1. Modify the example URLs with your credentials (dbuser & passwd) and host details.

1. Connect to MongoDB:

   ```sh
   mongo "mongodb://dbuser:passwd@your_host:27017"
   ```

1. List all databases:

   ```sh
   show dbs
   ```

1. View collections in a database:

  1. Switch to the database:

      ```sh
      use your_database_name
      ```

  1. Display its collections:

      ```sh
      show collections
      ```

1. Disconnect:

   ```sh
   exit
   ```

> You need the database and collection names to configure the source.

### Prepare your data

Data in MongoDB is stored in BSON (Binary JSON) format, which allows for embedded documents or
nested data. It employs a flexible schema, and its key terms include:

`Documents`: Key-value pairs representing data units.

`Collections`: Groups of documents, similar to database tables but without a fixed schema.

`Databases`: Containers for collections; a single MongoDB server can have multiple databases.

The `dlt` converts nested data into relational tables, deduces data types, and defines nested
relationships, creating an adaptive schema for future data adjustments.

### Initialize the verified source

To get started with your data pipeline, follow these steps:

1. Enter the following command:

   ```cli
   dlt init mongodb duckdb
   ```

   [This command](../../reference/command-line-interface) will initialize
   [the pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/mongodb_pipeline.py)
   with MongoDB as the [source](../../general-usage/source) and [duckdb](../destinations/duckdb.md)
   as the [destination](../destinations).

1. If you'd like to use a different destination, simply replace `duckdb` with the name of your
   preferred [destination](../destinations).

1. The command creates a directory with the files and the config that the pipeline needs.

### Add credentials

1. Inside the `.dlt` folder, you'll find a file called `secrets.toml`, which is where you can
   securely store your access tokens and other sensitive information. It's important to handle this
   file with care and keep it safe. Here's what the file looks like:

   ```toml
   # put your secret values and credentials here
   # do not share this file and do not push it to github
   [sources.mongodb]
   connection_url = "mongodb connection_url" # please set me up!
   ```

1. Replace the `connection_url` value with the [previously copied one](#grab-connection_url) to ensure
   secure access to your MongoDB sources.

1. Next, follow the [destination documentation](../../dlt-ecosystem/destinations) instructions to
   add credentials for your chosen destination, ensuring proper routing of your data to the final
   destination.

1. Next, store your config in the `.dlt/config.toml`.

   Here's what the `config.toml` looks like:

   ```toml
   [your_pipeline_name]  # Set your pipeline name here!
   database = "defaultDB"  # Database name (Optional), default database is loaded if not provided.
   collection_names = ["collection_1", "collection_2"] # Collection names (Optional), all collections are loaded if not provided.
   ```

   > Optionally, you can set database and collection names in ".dlt/secrets.toml" under
   > [sources.mongodb] without listing the pipeline name.

1. Replace the value of the "database" and "collections_names" with the ones
   [copied above](#grab-database-and-collections).

For more information, read the [General Usage: Credentials.](../../general-usage/credentials)

## Run the pipeline

1. Before running the pipeline, ensure that you have installed all the necessary dependencies by
   running the command:

   ```cli
   pip install -r requirements.txt
   ```

1. You're now ready to run the pipeline! To get started, run the following command:

   ```sh
   python mongodb_pipeline.py
   ```

1. Once the pipeline has finished running, you can verify that everything loaded correctly by using
   the following command:

   ```cli
   dlt pipeline <pipeline_name> show
   ```

   For example, the `pipeline_name` of the pipeline example is `local_mongo`. You can use a
   different name.

For more information, read the guide on [how to run a pipeline](../../walkthroughs/run-a-pipeline).

## Sources and resources

`dlt` works on the principle of [sources](../../general-usage/source) and
[resources](../../general-usage/resource).

### Source `mongodb`

This function loads data from a MongoDB database, yielding one or multiple collections to be retrieved.

```py
from typing import Any, Iterable, Literal, Mapping
from dlt.extract import DltResource

@dlt.source
def mongodb(
    connection_url: str = dlt.secrets.value,
    database: str | None = dlt.config.value,
    collection_names: list[str] | None = dlt.config.value,
    incremental: dlt.sources.incremental | None = None,  # type: ignore[type-arg]
    write_disposition: str | None = dlt.config.value,
    parallel: bool | None = dlt.config.value,
    limit: int | None = None,
    filter_: dict[str, Any] | None = None,
    projection: Mapping[str, Any] | Iterable[str] | None = None,
    pymongoarrow_schema: Any | None = None,
    chunk_size: int | None = 10000,
    backend: Literal["python", "pyarrow"] = "python",
    on_inferred_type_mismatch: Literal["ignore", "warn", "raise"] = "ignore",
    partition_size_bytes: int = 16 * 1024 * 1024,
) -> Iterable[DltResource]:
   ...
```

`connection_url`: The MongoDB connection URL.

`database`: The database name. If you do not set it, the source uses the default database of the
connection URL.

`collection_names`: The collections to load. If you do not set them, the source loads all
collections except `system.*`.

`incremental`: The incremental cursor of each collection, see
[Incremental loading](#incremental-loading).

`write_disposition`: The write disposition: "replace", "append", or "merge".

`parallel`: If `True`, the source reads partitions of each collection at the same time, see
[Parallel reads](#parallel-reads).

`limit`: The maximum number of documents that the source reads from each collection.

`filter_`: A MongoDB query filter. The source adds the incremental filter to it.

`projection`: The fields to read, as a list of names or as a MongoDB projection. The source always
reads the field of the incremental cursor.

`pymongoarrow_schema`: The types of the fields for the `pyarrow` backend, see
[pyarrow backend](#pyarrow-backend).

`chunk_size`: The number of documents in each batch. A parallel read loads each partition as one
batch.

`backend`: "python" (default) yields lists of dicts. "pyarrow" yields Arrow tables, see
[pyarrow backend](#pyarrow-backend).

`on_inferred_type_mismatch`: What the `pyarrow` backend does with values that do not have the type
of their field: "ignore" (default), "warn", or "raise".

`partition_size_bytes`: The maximum size of a partition of a parallel read, 16 MiB by default.

### Resource `mongodb_collection`

This resource reads one collection. It takes the arguments of the `mongodb` source, with
`collection` in place of `collection_names`.

```py
from typing import Any, Iterable, Literal, Mapping
from dlt.extract import DltResource

def mongodb_collection(
    connection_url: str = dlt.secrets.value,
    database: str | None = dlt.config.value,
    collection: str = dlt.config.value,
    incremental: dlt.sources.incremental | None = None,  # type: ignore[type-arg]
    write_disposition: str | None = dlt.config.value,
    parallel: bool | None = False,
    limit: int | None = None,
    chunk_size: int | None = 10000,
    backend: Literal["python", "pyarrow"] = "python",
    filter_: dict[str, Any] | None = None,
    projection: Mapping[str, Any] | Iterable[str] | None = dlt.config.value,
    pymongoarrow_schema: Any | None = None,
    on_inferred_type_mismatch: Literal["ignore", "warn", "raise"] = "ignore",
    partition_size_bytes: int = 16 * 1024 * 1024,
    data_item_format: Literal["object", "arrow"] | None = None,
) -> DltResource:
   ...
```

`collection`: The name of the collection to load.

`data_item_format`: Deprecated. "object" selects the `python` backend and "arrow" selects the
`pyarrow` backend.

## Customization

### Create your own pipeline

If you wish to create your own pipelines, you can leverage source and resource methods from this verified source.

1. Configure the pipeline by specifying the pipeline name, destination, and dataset as follows:

   ```py
   pipeline = dlt.pipeline(
       pipeline_name="mongodb_pipeline",  # Use a custom name if desired
       destination="duckdb",  # Choose the appropriate destination, for example duckdb, redshift or postgres
       dataset_name="mongodb_data"  # Use a custom name if desired
   )
   ```

1. To load all the collections in a database:

   ```py notype
   load_data = mongodb()
   load_info = pipeline.run(load_data, write_disposition="replace")
   print(load_info)
   ```

1. To load specific collections from the database:

   ```py notype
   load_data = mongodb().with_resources("collection_1", "collection_2")
   load_info = pipeline.run(load_data, write_disposition="replace")
   print(load_info)
   ```

1. To load specific collections from the source incrementally:

   ```py notype
   load_data = mongodb(incremental=dlt.sources.incremental("date")).with_resources("collection_1")
   load_info = pipeline.run(load_data, write_disposition="merge")
   print(load_info)
   ```

   > Data is loaded incrementally based on the "date" field.

1. To load data from a particular collection, say "movies," incrementally:

   ```py notype
   load_data = mongodb_collection(
       collection="movies",
       incremental=dlt.sources.incremental(
           "lastupdated", initial_value=datetime(2020, 9, 10, tzinfo=timezone.utc)
     ))

   load_info = pipeline.run(load_data, write_disposition="merge")

   ```

   > The source function "mongodb_collection" loads data from a particular single
   > collection, whereas the source "mongodb" can load data from multiple collections.
   > This script configures incremental loading from the "movies" collection based on the
   > "lastupdated" field, starting from midnight on September 10, 2020.

1. To incrementally load a table with an append-only disposition using hints:

   ```py notype
   # Suitable for tables where new rows are added, but existing rows aren't updated.
   # Load data from the 'listingsAndReviews' collection in MongoDB, using 'last_scraped' for incremental addition.
   airbnb = mongodb().with_resources("listingsAndReviews")

   airbnb.listingsAndReviews.apply_hints(
       incremental=dlt.sources.incremental("last_scraped")
   )
   info = pipeline.run(airbnb, write_disposition="append")

   ```

   > It applies a hint for incremental loading based on the "last_scraped" field, ideal for tables
   > with additions but no updates.

1. To load a selected collection and rename it in the destination:

   ```py notype
   # Create the MongoDB source and select the "collection_1" collection
   source = mongodb().with_resources("collection_1")

   # Apply the hint to rename the table in the destination
   source.resources["collection_1"].apply_hints(table_name="loaded_data_1")

   # Run the pipeline
   info = pipeline.run(source, write_disposition="replace")
   print(info)
   ```

1. To load a selected collection as Arrow tables:

   ```py notype
   # Load collection "movies" with the pyarrow backend
   movies = mongodb_collection(
      collection="movies",
      backend="pyarrow",
   )

   # Run the pipeline
   info = pipeline.run(movies)
   print(info)
   ```

## Incremental loading

The source sends the incremental cursor to the server as a query filter. The filter uses
`initial_value`, `end_value`, `range_start`, `range_end`, and `on_cursor_value_missing="include"`.
A `projection` always includes the field of the cursor.

If you set `row_order`, the server sorts the documents by the cursor. The direction of the order
depends on `last_value_func`: "asc" gives ascending values for `max` and descending values for
`min`.

With `row_order`, you can load a large collection in chunks with `add_limit`. Each run continues
from the last value of the cursor. Set `parallel=False` for this, because a parallel read has no
order.

## Primary key

The resources use `_id` as the primary key. Views and time series collections can return documents
without `_id`, and the load then fails. Set another primary key, or none:

```py notype
measurements = mongodb_collection(collection="measurements")
measurements.apply_hints(primary_key=())
```

## Optimization

Three settings change the speed of a load: network compression, the `pyarrow` backend, and parallel
reads. This table shows the time to read one collection from MongoDB Atlas over the internet. The
time of the slowest combination is 100%:

| Backend   | Parallel reads | No compression    | zstd compression (default) |
| --------- | -------------- | ----------------- | -------------------------- |
| `python`  | no             | 100%              | 39% (2.6x faster)          |
| `python`  | 8 workers      | 60% (1.7x faster) | 18% (5.5x faster)          |
| `pyarrow` | no             | 96%               | 26% (3.9x faster)          |
| `pyarrow` | 8 workers      | 59% (1.7x faster) | 14% (7.4x faster)          |

A server on the same machine or network gives different results. With a local server, the CPU sets
the speed, not the network. Compression makes these loads about 30% slower.

This table shows the time to read one collection from a MongoDB server on the same machine, without
compression. The time of the slowest combination is 100%:

| Backend   | No parallel reads | 4 workers         | 8 workers         |
| --------- | ----------------- | ----------------- | ----------------- |
| `python`  | 85% (1.2x faster) | 96%               | 100%              |
| `pyarrow` | 33% (3x faster)   | 15% (6.6x faster) | 13% (7.7x faster) |

The `pyarrow` backend parses the documents in worker processes, and each process uses one CPU core.
The `python` backend parses the documents in threads, which do not make it faster on a local server.

### Network compression

By default, the MongoDB server sends BSON documents without compression. The source asks the
server to compress the documents with zstd, snappy, or zlib. pymongo uses the first compressor that
the server supports and that is installed.

The requirements of the source install zstd. zlib is part of the Python standard library.
Compression uses CPU time on the server and on the client.

To use a different compressor, set `compressors` in the connection URL. The source then does not
change it:

```text
connection_url = "mongodb+srv://dbuser:passwd@cluster.mongodb.net/?compressors=zlib"
```

If the server is on the same machine or network, disable the compression. Add `compressors=` without
a value to the connection URL:

```text
connection_url = "mongodb://dbuser:passwd@host.or.ip:27017/?compressors="
```

### pyarrow backend

With `backend="pyarrow"`, the source parses the documents into Arrow tables with
[pymongoarrow](https://www.mongodb.com/docs/languages/python/pymongo-arrow-driver/current/). This
backend is faster than the `python` backend.

To use the `pyarrow` backend, install `pymongoarrow`:

```sh
pip install pymongoarrow
```

If `pymongoarrow` is not installed, the source logs a warning and uses the `python` backend. The
source works with `pymongoarrow` 1.5 and later, except 1.6.0 to 1.6.3. Each `pymongoarrow` release
works with one version of `pyarrow`.

Both backends load the same values:

* ObjectIds load as strings of 24 hexadecimal characters, also in nested documents and arrays.
* Dates load as timestamps in UTC.
* Decimal128 values load as text. For decimals, set the type of the field to `pa.decimal128` in
  `pymongoarrow_schema`.
* Binary values load as binary. JavaScript code loads as text.
* Nested documents become columns up to `max_table_nesting`, for example `address__city`.

Arrays load as nested tables with the `python` backend. With the `pyarrow` backend, arrays load as
`json` columns. On destinations that support nested types, for example `filesystem` with Parquet,
they load as nested types. To change the backend of a loaded collection, load it again, for example
with `refresh="drop_resources"`.

pymongoarrow gets the type of a field from its first value. It converts numbers of other types to
this type, for example 2.5 loads as 2 in an integer field. It loads values of other types as null.
The source does the same when the type of a field changes between batches. The
`on_inferred_type_mismatch` argument sets what the source does then:

* "ignore" (default) loads these values without a warning.
* "warn" loads these values and logs a warning.
* "raise" stops the load with `FieldTypeMismatchError`.

"warn" and "raise" parse each batch a second time and need `pymongoarrow` 1.9 or later.

To set the types of the fields yourself, set `pymongoarrow_schema`. The source then reads only these
fields. With `pymongoarrow` 1.9 or later, values that do not match the schema stop the load with
`FieldTypeMismatchError`.

### Parallel reads

With `parallel=True`, the source splits each collection into partitions of `_id` values. Then it
reads the partitions at the same time. A partition holds about `partition_size_bytes` of documents.
The source computes the number of documents from the average document size of the collection.
Partitions are smaller, down to 1 MiB, when each extract worker gets fewer than 8 partitions.

The `workers` setting of the `extract` config sets the number of partitions that the source reads at
the same time, for all collections together. The default is 5. To read 8 partitions at the same
time, set `workers` in `config.toml`:

```toml
[extract]
workers = 8
```

The source sizes the partitions from this config. The `workers` argument of `pipeline.extract` does
not change the size of the partitions.

With the `pyarrow` backend, worker processes parse the partitions. Each extract worker starts one
worker process, so all collections share at most `workers` processes. Each worker process uses one
CPU core. Thus, more workers give a faster load until the network or the server sets the speed.

With the `python` backend, threads parse the documents. Python runs these threads one at a time.
Thus, parallel reads make the `python` backend faster only if the network sets the speed.

The source reads a collection in one query in these cases:

* Views, time series collections, and small collections.
* A `limit`.
* The server rejects the queries that plan the partitions, for example `$where` in `filter_`. The
  source then logs a warning.

With a filter, for example an incremental with a last value, the source counts and samples the
matching documents to plan the partitions. To make this fast, create an index on the cursor field.

If the `_id` values of a collection have different BSON types, the source splits the most common
type into partitions. It reads each other type in one query.

The worker processes of the `pyarrow` backend start with the Python interpreter of the pipeline. If
the pipeline runs in an embedded interpreter, for example uWSGI, set `parallel=False`.

<!--@@@DLT_TUBA mongodb-->
