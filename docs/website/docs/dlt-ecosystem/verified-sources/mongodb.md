---
title: MongoDB
description: dlt verified source for MongoDB
keywords: [mongodb, verified source, mongo database]
---
import Header from './_source-info-header.md';

# MongoDB

<Header/>

[MongoDB](https://www.mongodb.com/what-is-mongodb) is a NoSQL database that stores JSON-like
documents.

This MongoDB `dlt` verified source and
[pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/mongodb_pipeline.py)
loads data using the "MongoDB" source to the destination of your choice.

Sources and resources that can be loaded using this verified source are:

| Name               | Description                                |
|--------------------|--------------------------------------------|
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
|---------------------|---------------------------------------------------------------------------------------|---------------------------------------------------|
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

> Note the database and collection names for future source configuration.

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

   ```sh
   dlt init mongodb duckdb
   ```

   [This command](../../reference/command-line-interface) will initialize
   [the pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/mongodb_pipeline.py)
   with MongoDB as the [source](../../general-usage/source) and [duckdb](../destinations/duckdb.md)
   as the [destination](../destinations).

1. If you'd like to use a different destination, simply replace `duckdb` with the name of your
   preferred [destination](../destinations).

1. After running this command, a new directory will be created with the necessary files and
   configuration settings to get started.

For more information, read the guide on [how to add a verified source](../../walkthroughs/add-a-verified-source).

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

1. Next, store your configuration details in the `.dlt/config.toml`.

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
   ```sh
   pip install -r requirements.txt
   ```
1. You're now ready to run the pipeline! To get started, run the following command:
   ```sh
   python mongodb_pipeline.py
   ```
1. Once the pipeline has finished running, you can verify that everything loaded correctly by using
   the following command:
   ```sh
   dlt pipeline <pipeline_name> show
   ```
   For example, the `pipeline_name` for the above pipeline example is `local_mongo`, you may also
   use any custom name instead.

For more information, read the guide on [how to run a pipeline](../../walkthroughs/run-a-pipeline).

## Sources and resources

`dlt` works on the principle of [sources](../../general-usage/source) and
[resources](../../general-usage/resource).

### Source `mongodb`

This function loads data from a MongoDB database, yielding one or multiple collections to be retrieved.

```py
@dlt.source
def mongodb(
    connection_url: str = dlt.secrets.value,
    database: Optional[str] = dlt.config.value,
    collection_names: Optional[List[str]] = dlt.config.value,
    incremental: Optional[dlt.sources.incremental] = None,  # type: ignore[type-arg]
    write_disposition: Optional[str] = dlt.config.value,
) -> Iterable[DltResource]:
   ...
```

`connection_url`: MongoDB connection URL.

`database`: Database name (defaults if unspecified).

`collection_names`: Names of desired collections; loads all if not specified.

`incremental`: Option for incremental data loading.

`write_disposition`: Writing mode: "replace", "append", or "merge".


### Source `mongo_collection`

This function fetches a single collection from a MongoDB database using PyMongo.

```py
def mongodb_collection(
    connection_url: str = dlt.secrets.value,
    database: Optional[str] = dlt.config.value,
    collection: str = dlt.config.value,
    incremental: Optional[dlt.sources.incremental] = None,  # type: ignore[type-arg]
    write_disposition: Optional[str] = dlt.config.value,
) -> Any:
   ...
```

`collection`: Name of the collection to load.


## Customization

### Create your own pipeline

If you wish to create your own pipelines, you can leverage source and resource methods from this verified source.

1. Configure the pipeline by specifying the pipeline name, destination, and dataset as follows:

   ```py
   pipeline = dlt.pipeline(
       pipeline_name="mongodb_pipeline",  # Use a custom name if desired
       destination="duckdb",  # Choose the appropriate destination (e.g., duckdb, redshift, post)
       dataset_name="mongodb_data"  # Use a custom name if desired
   )
   ```

1. To load all the collections in a database:

   ```py
   load_data = mongodb()
   load_info = pipeline.run(load_data, write_disposition="replace")
   print(load_info)
   ```

1. To load specific collections from the database:

   ```py
   load_data = mongodb().with_resources("collection_1", "collection_2")
   load_info = pipeline.run(load_data, write_disposition="replace")
   print(load_info)
   ```

1. To load specific collections from the source incrementally:

   ```py
   load_data = mongodb(incremental=dlt.sources.incremental("date")).with_resources("collection_1")
   load_info = pipeline.run(load_data, write_disposition="merge")
   print(load_info)
   ```
   > Data is loaded incrementally based on the "date" field.

1. To load data from a particular collection, say "movies," incrementally:

   ```py
   load_data = mongodb_collection(
       collection="movies",
       incremental=dlt.sources.incremental(
           "lastupdated", initial_value=pendulum.DateTime(2020, 9, 10, 0, 0, 0)
     ))

   load_info = pipeline.run(load_data, write_disposition="merge")

   ```

   > The source function "mongodb_collection" loads data from a particular single
   > collection, whereas the source "mongodb" can load data from multiple collections.
   > This script configures incremental loading from the "movies" collection based on the
   > "lastupdated" field, starting from midnight on September 10, 2020.

1. To incrementally load a table with an append-only disposition using hints:

   ```py
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

   ```py
   # Create the MongoDB source and select the "collection_1" collection
   source = mongodb().with_resources("collection_1")

   # Apply the hint to rename the table in the destination
   source.resources["collection_1"].apply_hints(table_name="loaded_data_1")

   # Run the pipeline
   info = pipeline.run(source, write_disposition="replace")
   print(info)
   ```

1. To load a selected collection, using Apache Arrow for data conversion:
   ```py
   # Load collection "movies", using Apache Arrow for conversion
   movies = mongodb_collection(
      collection="movies",
      data_item_format="arrow",
   )

   # Run the pipeline
   info = pipeline.run(movies)
   print(info)
   ```

<!--@@@DLT_TUBA mongodb-->

