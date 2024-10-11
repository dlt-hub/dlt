---
title: ClickHouse
description: ClickHouse `dlt` destination
keywords: [ clickhouse, destination, data warehouse ]
---

# ClickHouse

## Install dlt with ClickHouse

**To install the DLT library with ClickHouse dependencies:**

```sh
pip install "dlt[clickhouse]"
```

## Setup guide

### 1. Initialize the dlt project

Let's start by initializing a new `dlt` project as follows:

```sh
dlt init chess clickhouse
```

> 💡 This command will initialize your pipeline with chess as the source and ClickHouse as the destination.

The above command generates several files and directories, including `.dlt/secrets.toml` and a requirements file for ClickHouse. You can install the necessary dependencies specified in the requirements file by executing it as follows:

```sh
pip install -r requirements.txt
```

or with `pip install "dlt[clickhouse]"`, which installs the `dlt` library and the necessary dependencies for working with ClickHouse as a destination.

### 2. Setup ClickHouse database

To load data into ClickHouse, you need to create a ClickHouse database. While we recommend asking our GPT-4 assistant for details, we've provided a general outline of the process below:

1. You can use an existing ClickHouse database or create a new one.

2. To create a new database, connect to your ClickHouse server using the `clickhouse-client` command line tool or a SQL client of your choice.

3. Run the following SQL commands to create a new database, user, and grant the necessary permissions:

   ```sql
   CREATE DATABASE IF NOT EXISTS dlt;
   CREATE USER dlt IDENTIFIED WITH sha256_password BY 'Dlt*12345789234567';
   GRANT CREATE, ALTER, SELECT, DELETE, DROP, TRUNCATE, OPTIMIZE, SHOW, INSERT, dictGet ON dlt.* TO dlt;
   GRANT SELECT ON INFORMATION_SCHEMA.COLUMNS TO dlt;
   GRANT CREATE TEMPORARY TABLE, S3 ON *.* TO dlt;
   ```

### 3. Add credentials

1. Next, set up the ClickHouse credentials in the `.dlt/secrets.toml` file as shown below:

   ```toml
   [destination.clickhouse.credentials]
   database = "dlt"                         # The database name you created.
   username = "dlt"                         # ClickHouse username, default is usually "default".
   password = "Dlt*12345789234567"          # ClickHouse password, if any.
   host = "localhost"                       # ClickHouse server host.
   port = 9000                              # ClickHouse native TCP protocol port, default is 9000.
   http_port = 8443                         # ClickHouse HTTP port, default is 9000.
   secure = 1                               # Set to 1 if using HTTPS, else 0.
   ```

   :::info Network Ports
    The `http_port` parameter specifies the port number to use when connecting to the ClickHouse server's HTTP interface.
    The default non-secure HTTP port for ClickHouse is `8123`.
    This is different from the default port `9000`, which is used for the native TCP protocol.

    You must set `http_port` if you are not using external staging (i.e., you don't set the `staging` parameter in your pipeline). This is because dlt's built-in ClickHouse local storage staging uses the [clickhouse-connect](https://github.com/ClickHouse/clickhouse-connect) library, which communicates with ClickHouse over HTTP.

    Make sure your ClickHouse server is configured to accept HTTP connections on the port specified by `http_port`. For example:

   - If you set `http_port = 8123` (default non-secure HTTP port), then ClickHouse should be listening for HTTP requests on port 8123.
   - If you set `http_port = 8443`, then ClickHouse should be listening for secure HTTPS requests on port 8443.

   If you're using external staging, you can omit the `http_port` parameter, since clickhouse-connect will not be used in this case.

   For local development and testing with ClickHouse running locally, it is recommended to use the default non-secure HTTP port `8123` by setting `http_port=8123` or omitting the parameter.

   Please see the [ClickHouse network port documentation](https://clickhouse.com/docs/en/guides/sre/network-ports) for further reference.
   :::

2. You can pass a database connection string similar to the one used by the `clickhouse-driver` library. The credentials above will look like this:

   ```toml
   # Keep it at the top of your TOML file, before any section starts
   destination.clickhouse.credentials="clickhouse://dlt:Dlt*12345789234567@localhost:9000/dlt?secure=1"
   ```

### 3. Add configuration options

You can set the following configuration options in the `.dlt/secrets.toml` file:

```toml
[destination.clickhouse]
dataset_table_separator = "___"                         # The default separator for dataset table names from the dataset.
table_engine_type = "merge_tree"                        # The default table engine to use.
dataset_sentinel_table_name = "dlt_sentinel_table"      # The default name for sentinel tables.
staging_use_https = true                                # Wether to connecto to the staging bucket via https (defaults to True)
```

## Write disposition

All [write dispositions](../../general-usage/incremental-loading#choosing-a-write-disposition) are supported.

## Data loading

Data is loaded into ClickHouse using the most efficient method depending on the data source:

- For local files, the `clickhouse-connect` library is used to directly load files into ClickHouse tables using the `INSERT` command.
- For files in remote storage like S3, Google Cloud Storage, or Azure Blob Storage, ClickHouse table functions like `s3`, `gcs`, and `azureBlobStorage` are used to read the files and insert the data into tables.

## Datasets

`Clickhouse` does not support multiple datasets in one database; dlt relies on datasets to exist for multiple reasons.
To make `clickhouse` work with `dlt`, tables generated by `dlt` in your `clickhouse` database will have their names prefixed with the dataset name, separated by
the configurable `dataset_table_separator`.
Additionally, a special sentinel table that doesn't contain any data will be created, so dlt knows which virtual datasets already exist in a
clickhouse
destination.

## Supported file formats

- [jsonl](../file-formats/jsonl.md) is the preferred format for both direct loading and staging.
- [parquet](../file-formats/parquet.md) is supported for both direct loading and staging.

The `clickhouse` destination has a few specific deviations from the default SQL destinations:

1. `Clickhouse` has an experimental `object` datatype, but we've found it to be a bit unpredictable, so the dlt clickhouse destination will load the `json` datatype to a `text` column.
   If you need
   this feature, get in touch with our Slack community, and we will consider adding it.
2. `Clickhouse` does not support the `time` datatype. Time will be loaded to a `text` column.
3. `Clickhouse` does not support the `binary` datatype. Binary will be loaded to a `text` column. When loading from `jsonl`, this will be a base64 string; when loading from parquet, this will be
   the `binary` object converted to `text`.
4. `Clickhouse` accepts adding columns to a populated table that aren’t null.
5. `Clickhouse` can produce rounding errors under certain conditions when using the float/double datatype. Make sure to use decimal if you can’t afford to have rounding errors. Loading the value
   12.7001 to a double column with the loader file format jsonl set will predictably produce a rounding error, for example.

## Supported column hints

ClickHouse supports the following [column hints](../../general-usage/schema#tables-and-columns):

- `primary_key` - marks the column as part of the primary key. Multiple columns can have this hint to create a composite primary key.

## Choosing a table engine

dlt defaults to the `MergeTree` table engine. You can specify an alternate table engine in two ways:

### Setting a default table engine in the configuration

You can set a default table engine for all resources and dlt tables by adding the `table_engine_type` parameter to your ClickHouse credentials in the `.dlt/secrets.toml` file:

```toml
[destination.clickhouse]
# ... (other configuration options)
table_engine_type = "merge_tree"                        # The default table engine to use.
```

### Setting the table engine for specific resources

You can also set the table engine for specific resources using the clickhouse_adapter, which will override the default engine set in `.dlt/secrets.toml` for that resource:

```py
from dlt.destinations.adapters import clickhouse_adapter

@dlt.resource()
def my_resource():
    ...

clickhouse_adapter(my_resource, table_engine_type="merge_tree")
```

Supported values for `table_engine_type` are:

- `merge_tree` (default) - creates tables using the `MergeTree` engine, suitable for most use cases. [Learn more about MergeTree](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree).
- `shared_merge_tree` - creates tables using the `SharedMergeTree` engine, optimized for cloud-native environments with shared storage. This table is **only** available on ClickHouse Cloud, and it is the default selection if `merge_tree` is selected. [Learn more about SharedMergeTree](https://clickhouse.com/docs/en/cloud/reference/shared-merge-tree).
- `replicated_merge_tree` - creates tables using the `ReplicatedMergeTree` engine, which supports data replication across multiple nodes for high availability. [Learn more about ReplicatedMergeTree](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replication). This defaults to `shared_merge_tree` on ClickHouse Cloud.
- Experimental support for the `Log` engine family with `stripe_log` and `tiny_log`.

For local development and testing with ClickHouse running locally, the `MergeTree` engine is recommended.

## Staging support

ClickHouse supports Amazon S3, Google Cloud Storage, and Azure Blob Storage as file staging destinations.

`dlt` will upload Parquet or JSONL files to the staging location and use ClickHouse table functions to load the data directly from the staged files.

Please refer to the filesystem documentation to learn how to configure credentials for the staging destinations:

- [Amazon S3](./filesystem.md#aws-s3)
- [Google Cloud Storage](./filesystem.md#google-storage)
- [Azure Blob Storage](./filesystem.md#azure-blob-storage)

To run a pipeline with staging enabled:

```py
pipeline = dlt.pipeline(
  pipeline_name='chess_pipeline',
  destination='clickhouse',
  staging='filesystem',  # add this to activate staging
  dataset_name='chess_data'
)
```

### Using Google Cloud or S3-compatible storage as a staging area

dlt supports using S3-compatible storage services, including Google Cloud Storage (GCS), as a staging area when loading data into ClickHouse.
This is handled automatically by
ClickHouse's [GCS table function](https://clickhouse.com/docs/en/sql-reference/table-functions/gcs), which dlt uses under the hood.

The ClickHouse GCS table function only supports authentication using Hash-based Message Authentication Code (HMAC) keys, which is compatible with the Amazon S3 API.
To enable this, GCS provides an S3
compatibility mode that emulates the S3 API, allowing ClickHouse to access GCS buckets via its S3 integration.

For detailed instructions on setting up S3-compatible storage with dlt, including AWS S3, MinIO, and Cloudflare R2, refer to
the [dlt documentation on filesystem destinations](../../dlt-ecosystem/destinations/filesystem#using-s3-compatible-storage).

To set up GCS staging with HMAC authentication in dlt:

1. Create HMAC keys for your GCS service account by following the [Google Cloud guide](https://cloud.google.com/storage/docs/authentication/managing-hmackeys#create).

2. Configure the HMAC keys (`aws_access_key_id` and `aws_secret_access_key`) in your dlt project's ClickHouse destination settings in `config.toml`, similar to how you would configure AWS S3
   credentials:

```toml
[destination.filesystem]
bucket_url = "s3://my_awesome_bucket"

[destination.filesystem.credentials]
aws_access_key_id = "JFJ$$*f2058024835jFffsadf"
aws_secret_access_key = "DFJdwslf2hf57)%$02jaflsedjfasoi"
project_id = "my-awesome-project"
endpoint_url = "https://storage.googleapis.com"
```

:::caution
When configuring the `bucket_url` for S3-compatible storage services like Google Cloud Storage (GCS) with ClickHouse in dlt, ensure that the URL is prepended with `s3://` instead of `gs://`. This is
because the ClickHouse GCS table function requires the use of HMAC credentials, which are compatible with the S3 API. Prepending with `s3://` allows the HMAC credentials to integrate properly with
dlt's staging mechanisms for ClickHouse.
:::

### dbt support

Integration with [dbt](../transformations/dbt/dbt.md) is generally supported via dbt-clickhouse but not tested by us.

### Syncing of `dlt` state

This destination fully supports [dlt state sync](../../general-usage/state#syncing-state-with-destination).

<!--@@@DLT_TUBA clickhouse-->

